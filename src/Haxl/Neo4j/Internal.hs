module Haxl.Neo4j.Internal where

import Haxl.Neo4j.Batch (runBatchRequests,AnyNeo4jRequest(..))
import Haxl.Neo4j.Requests (
    Neo4jRequest(..),
    NodeId,Node)

import Pipes.HTTP (Manager,withManager,defaultManagerSettings)

import Data.Aeson (fromJSON,Result(Success),FromJSON,Value)

import Haxl.Prelude

import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

import Data.Function (on)



runNeo4j :: Neo4j a -> IO [a]
runNeo4j neo4j = do
    withManager defaultManagerSettings (\manager -> do
        environment <- initEnv (stateSet (Neo4jState manager) stateEmpty) ()
        runHaxl environment (unNeo4j neo4j))

nodeById :: NodeId -> Neo4j Node
nodeById = Neo4j . fmap (:[]) . dataFetch . NodeById



type Haxl = GenHaxl ()

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

-- | A neo4j node.
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


-- | A neo4j edge.
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

-- | The properties of either a node or an edge. A map from 'Text' keys to
--   json values.
type Properties = HashMap Text Value

-- | A label of either a node or an edge.
type Label = Text

type NodeId = Integer

type EdgeId = Integer

-- | Given a json value that should represent a URI parse the last part of
--   it as a number.
parseSelfId :: Value -> Parser Integer
parseSelfId = withText "URI" (\s -> case idSlug s of
    Left errormessage -> fail errormessage
    Right idslug      -> return idslug)

-- | Extract the last part of the given URI.
idSlug :: Text -> Either String Integer
idSlug uri = readErr ("Reading URI slug failed: " ++ uriSlug) uriSlug where
    uriSlug = unpack (Text.reverse (Text.takeWhile (/= '/') (Text.reverse uri)))

neo4jFetch :: State Neo4jRequest -> Flags -> u -> [BlockedFetch Neo4jRequest] -> PerformFetch
neo4jFetch (Neo4jState manager) _ _ blockedfetches = SyncFetch (do
    let neo4jrequests = map (\(BlockedFetch neo4jrequest _) -> AnyNeo4jRequest neo4jrequest) blockedfetches
    values <- runBatchRequests neo4jrequests manager
    forM_ (zip blockedfetches values) writeResult)

writeResult :: (BlockedFetch Neo4jRequest,Value) -> IO ()
writeResult (BlockedFetch neo4request resultvar,value) = case neo4request of
    NodeById _ -> do
        let Success result = fromJSON value
        putSuccess resultvar result

runBatchRequests :: [AnyNeo4jRequest] -> Manager -> IO [Value]
runBatchRequests neo4jrequests manager = withHTTP request manager handleResponse where
    Just requestUrl = parseUrl "http://localhost:7474/db/data/batch"
    request = requestUrl {
        method = "POST",
        requestHeaders = [
            (hAccept,"application/json; charset=UTF-8"),
            (hContentType,"application/json")],
        requestBody = stream (encode neo4jrequests)}
    handleResponse response = do
        Just (Right responsevalues) <- evalStateT decode (responseBody response)
        return responsevalues

data AnyNeo4jRequest = forall a . AnyNeo4jRequest (Neo4jRequest a)

instance ToJSON AnyNeo4jRequest where
    toJSON (AnyNeo4jRequest (NodeById nodeid)) = object [
        "method" .= ("GET" :: Text),
        "to" .= nodeURI nodeid]

-- | Find a node's URI.
nodeURI :: NodeId -> Text
nodeURI nodeid = "/node/" `append` (pack (show nodeid))

-- | Find an edge's URI.
edgeURI :: EdgeId -> Text
edgeURI edgeid = "/relationship/" `append` (pack (show edgeid))

