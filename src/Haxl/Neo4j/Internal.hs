{-# LANGUAGE ExistentialQuantification, OverloadedStrings,
    StandaloneDeriving, GADTs, FlexibleInstances, MultiParamTypeClasses,
    TypeFamilies, DeriveDataTypeable #-}

module Haxl.Neo4j.Internal where

import Haxl.Core (
    DataSource(fetch),DataSourceName(dataSourceName),StateKey,State,
    Flags,BlockedFetch(BlockedFetch),Show1(show1),
    GenHaxl,dataFetch,PerformFetch(SyncFetch),putSuccess,
    JSONError(JSONError),NotFound(NotFound),putFailure,
    initEnv,stateSet,stateEmpty,runHaxl)
import Haxl.Prelude (forM_)

import Pipes.HTTP (
    Manager,withManager,defaultManagerSettings,
    withHTTP,parseUrl,Request(method,requestHeaders,requestBody),
    stream,responseBody,responseStatus)
import Network.HTTP.Types.Header (
    hAccept,hContentType)
import Network.HTTP.Types.Status (
    ok200)
import Network.HTTP.Types.URI (
    encodePathSegments)

import Data.Aeson (
    Value,
    FromJSON(parseJSON),fromJSON,Result(Success,Error),
    withObject,withText,(.:),
    ToJSON(toJSON),object,(.=))
import Data.Aeson.Types (
    Parser)

import Pipes.Prelude (toListM)
import Pipes.Aeson.Unchecked (encode,decode)
import Pipes.Parse (evalStateT)

import Control.Error (readErr)

import Data.Typeable (Typeable,Typeable1)
import Data.Hashable (Hashable(hashWithSalt))

import Data.HashMap.Lazy (HashMap)

import Data.Text (Text)
import qualified Data.Text as Text (
    unpack,reverse,takeWhile,pack)
import qualified Data.Text.Encoding as Text (
    decodeUtf8)
import qualified Data.ByteString as ByteString (
    concat)
import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Builder (
    toByteString)

import Data.Function (on)



type Haxl = GenHaxl ()

runHaxlNeo4j :: Haxl a -> IO a
runHaxlNeo4j neo4j = withManager defaultManagerSettings (\manager -> do
    environment <- initEnv (stateSet (Neo4jState manager) stateEmpty) ()
    runHaxl environment neo4j)


nodeById :: NodeId -> Haxl Node
nodeById = dataFetch . NodeById

nodesByLabel :: Label -> Haxl [Node]
nodesByLabel = dataFetch . NodesByLabel

edgeById :: EdgeId -> Haxl Edge
edgeById = dataFetch . EdgeById

incomingEdges :: NodeId -> Haxl [Edge]
incomingEdges = dataFetch . IncomingEdges

outgoingEdges :: NodeId -> Haxl [Edge]
outgoingEdges = dataFetch . OutgoingEdges

allEdges :: NodeId -> Haxl [Edge]
allEdges = dataFetch . AllEdges

incomingTypedEdges :: Label -> NodeId -> Haxl [Edge]
incomingTypedEdges = (dataFetch .) . IncomingTypedEdges

outgoingTypedEdges :: Label -> NodeId -> Haxl [Edge]
outgoingTypedEdges = (dataFetch .) . OutgoingTypedEdges

allTypedEdges :: Label -> NodeId -> Haxl [Edge]
allTypedEdges = (dataFetch .) . AllTypedEdges

nodeLabels :: NodeId -> Haxl [Label]
nodeLabels = dataFetch . NodeLabels


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
    result <- runBatchRequests someneo4jrequests manager
    case result of
        Left message -> forM_ blockedfetches (writeFailure message)
        Right responses -> forM_ (zip blockedfetches responses) writeResult)

runBatchRequests :: [SomeNeo4jRequest] -> Manager -> IO (Either Text [SomeNeo4jResponse])
runBatchRequests someneo4jrequests manager = withHTTP request manager handleResponse where
    Just requestUrl = parseUrl "http://localhost:7474/db/data/batch"
    request = requestUrl {
        method = "POST",
        requestHeaders = [
            (hAccept,"application/json; charset=UTF-8"),
            (hContentType,"application/json")],
        requestBody = stream (encode someneo4jrequests)}
    handleResponse response = do
        if responseStatus response == ok200
            then do
                Just (Right responsevalues) <- evalStateT decode (responseBody response)
                return (Right responsevalues)
            else do
                responsechunks <- toListM (responseBody response)
                return (Left (Text.decodeUtf8 (ByteString.concat responsechunks)))

writeResult :: (BlockedFetch Neo4jRequest,SomeNeo4jResponse) -> IO ()
writeResult (BlockedFetch neo4request resultvar,SomeNeo4jResponse value) = case neo4request of
    NodeById _ -> case fromJSON value of
        Success result -> putSuccess resultvar result
        Error message  -> putFailure resultvar (JSONError (Text.pack message))

writeFailure :: Text -> BlockedFetch Neo4jRequest -> IO ()
writeFailure message (BlockedFetch _ resultvar) =
    putFailure resultvar (NotFound message)


data SomeNeo4jRequest = forall a . SomeNeo4jRequest (Neo4jRequest a)

instance ToJSON SomeNeo4jRequest where
    toJSON (SomeNeo4jRequest neo4jrequest) = object [
        "method" .= ("GET" :: Text),
        "to" .= Text.decodeUtf8 (Builder.toByteString (neo4jRequestUri neo4jrequest))]

neo4jRequestUri :: Neo4jRequest a -> Builder
neo4jRequestUri (NodeById nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid)]
neo4jRequestUri (NodesByLabel label) =
    encodePathSegments ["label",label,"nodes"]
neo4jRequestUri (NodesByLabelAndProperty _ _ _) =
    error "unsupported: NodesByLabelAndProperty"
neo4jRequestUri (EdgeById edgeid) =
    encodePathSegments ["relationship",Text.pack (show edgeid)]
neo4jRequestUri (IncomingEdges nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","in"]
neo4jRequestUri (OutgoingEdges nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","out"]
neo4jRequestUri (AllEdges nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","all"]
neo4jRequestUri (IncomingTypedEdges label nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","in",label]
neo4jRequestUri (OutgoingTypedEdges label nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","out",label]
neo4jRequestUri (AllTypedEdges label nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"relationships","all",label]
neo4jRequestUri (NodeLabels nodeid) =
    encodePathSegments ["node",Text.pack (show nodeid),"labels"]


data SomeNeo4jResponse = SomeNeo4jResponse Value

instance FromJSON SomeNeo4jResponse where
    parseJSON = withObject "BatchResponse" (\o -> do
        body <- o .: "body"
        return (SomeNeo4jResponse body))
