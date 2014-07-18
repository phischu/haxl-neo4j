{-# LANGUAGE ExistentialQuantification, OverloadedStrings,
    StandaloneDeriving, GADTs, FlexibleInstances, MultiParamTypeClasses,
    TypeFamilies, DeriveDataTypeable #-}

module Haxl.Neo4j.Internal where

import Haxl.Core (
    DataSource(fetch),DataSourceName(dataSourceName),StateKey,State,
    Flags,BlockedFetch(BlockedFetch),Show1(show1),
    GenHaxl,dataFetch,PerformFetch(SyncFetch),putSuccess,
    initEnv,stateSet,stateEmpty,runHaxl)
import Haxl.Prelude (forM_)

import Pipes.HTTP (
    Manager,withManager,defaultManagerSettings,
    withHTTP,parseUrl,Request(method,requestHeaders,requestBody),
    stream,responseBody)
import Network.HTTP.Types.Header (
    hAccept,hContentType)

import Data.Aeson (
    Value,
    FromJSON(parseJSON),fromJSON,Result(Success),
    withObject,withText,(.:),
    ToJSON(toJSON),object,(.=))
import Data.Aeson.Types (
    Parser)

import Pipes.Aeson.Unchecked (encode,decode)
import Pipes.Parse (evalStateT)

import Control.Error (readErr)

import Data.Typeable (Typeable,Typeable1)
import Data.Hashable (Hashable(hashWithSalt))

import Data.HashMap.Lazy (HashMap)

import Data.Text (Text)
import qualified Data.Text as Text (
    unpack,reverse,takeWhile,append,pack)

import Data.Function (on)



type Haxl = GenHaxl ()

runHaxlNeo4j :: Haxl a -> IO a
runHaxlNeo4j neo4j = withManager defaultManagerSettings (\manager -> do
    environment <- initEnv (stateSet (Neo4jState manager) stateEmpty) ()
    runHaxl environment neo4j)


nodeById :: NodeId -> Haxl Node
nodeById = dataFetch . NodeById


-- Neo4j request data type

data Neo4jRequest a where
    NodeById :: NodeId -> Neo4jRequest Node
    NodesByLabel :: Label -> Neo4jRequest [Node]
    NodesByLabelAndProperty :: Label -> Text -> Value -> Neo4jRequest [Node]
    EdgeById :: EdgeId -> Neo4jRequest Edge
    IncomingEdges :: NodeId -> Neo4jRequest [Edge]
    OutgoingEdges :: NodeId -> Neo4jRequest [Edge]
    AllEdges :: NodeId -> Neo4jRequest [Edge]
    IncomingTypedEdges :: Label -> NodeId -> Neo4jRequest [Edge]
    OutgoingTypedEdges :: Label -> NodeId -> Neo4jRequest [Edge]
    AllTypedEdges :: Label -> NodeId -> Neo4jRequest [Edge]
    NodeLabels :: NodeId -> Neo4jRequest [Label]

deriving instance Show (Neo4jRequest a)
deriving instance Eq (Neo4jRequest a)
deriving instance Typeable1 Neo4jRequest

instance DataSource u Neo4jRequest where
    fetch = neo4jFetch

instance Show1 Neo4jRequest where
    show1 = show

instance DataSourceName Neo4jRequest where
    dataSourceName _ = "Neo4j"

instance StateKey Neo4jRequest where
    data State Neo4jRequest = Neo4jState Manager

instance Hashable (Neo4jRequest a) where
    hashWithSalt s (NodeById i) = hashWithSalt s (0::Int,i)


-- Neo4j result data types

type NodeId = Integer

type EdgeId = Integer

type Properties = HashMap Text Value

type Label = Text

data Node = Node {
    nodeId :: NodeId,
    nodeData :: Properties}

deriving instance Show Node
deriving instance Typeable Node

instance Eq Node where
    (==) = (==) `on` nodeId

instance Ord Node where
    (<=) = (<=) `on` nodeId

instance FromJSON Node where
    parseJSON = withObject "NodeObject" (\o -> do
        selfid   <- o .: "self" >>= parseSelfId
        nodedata <- o .: "data"
        return (Node selfid nodedata))

data Edge = Edge {
    edgeId :: EdgeId,
    edgeStart :: NodeId,
    edgeEnd :: NodeId,
    edgeType :: Label,
    edgeData :: Properties}

deriving instance Show Edge
deriving instance Typeable Edge

instance Eq Edge where
    (==) = (==) `on` edgeId

instance Ord Edge where
    (<=) = (<=) `on` edgeId

instance FromJSON Edge where
    parseJSON = withObject "EdgeObject" (\o -> do
        selfid   <- o .: "self"  >>= parseSelfId
        startid  <- o .: "start" >>= parseSelfId
        endid    <- o .: "end"   >>= parseSelfId
        label    <- o .: "type"
        edgedata <- o .: "data"
        return (Edge selfid startid endid label edgedata))

parseSelfId :: Value -> Parser Integer
parseSelfId = withText "URI" (\s -> case idSlug s of
    Left errormessage -> fail errormessage
    Right idslug      -> return idslug)

idSlug :: Text -> Either String Integer
idSlug uri = readErr ("Reading URI slug failed: " ++ uriSlug) uriSlug where
    uriSlug = Text.unpack (Text.reverse (Text.takeWhile (/= '/') (Text.reverse uri)))


-- | Actually execute neo4j requests.

neo4jFetch :: State Neo4jRequest -> Flags -> u -> [BlockedFetch Neo4jRequest] -> PerformFetch
neo4jFetch (Neo4jState manager) _ _ blockedfetches = SyncFetch (do
    let someneo4jrequests = map (\(BlockedFetch neo4jrequest _) -> SomeNeo4jRequest neo4jrequest) blockedfetches
    values <- runBatchRequests someneo4jrequests manager
    forM_ (zip blockedfetches values) writeResult)


data SomeNeo4jRequest = forall a . SomeNeo4jRequest (Neo4jRequest a)

instance ToJSON SomeNeo4jRequest where
    toJSON (SomeNeo4jRequest (NodeById nodeid)) = object [
        "method" .= ("GET" :: Text),
        "to" .= nodeURI nodeid]

-- | Find a node's URI.
nodeURI :: NodeId -> Text
nodeURI nodeid = "/node/" `Text.append` (Text.pack (show nodeid))

-- | Find an edge's URI.
edgeURI :: EdgeId -> Text
edgeURI edgeid = "/relationship/" `Text.append` (Text.pack (show edgeid))


runBatchRequests :: [SomeNeo4jRequest] -> Manager -> IO [Value]
runBatchRequests someneo4jrequests manager = withHTTP request manager handleResponse where
    Just requestUrl = parseUrl "http://localhost:7474/db/data/batch"
    request = requestUrl {
        method = "POST",
        requestHeaders = [
            (hAccept,"application/json; charset=UTF-8"),
            (hContentType,"application/json")],
        requestBody = stream (encode someneo4jrequests)}
    handleResponse response = do
        Just (Right responsevalues) <- evalStateT decode (responseBody response)
        return responsevalues

writeResult :: (BlockedFetch Neo4jRequest,Value) -> IO ()
writeResult (BlockedFetch neo4request resultvar,value) = case neo4request of
    NodeById _ -> do
        let Success result = fromJSON value
        putSuccess resultvar result



