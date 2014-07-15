{-# LANGUAGE StandaloneDeriving,OverloadedStrings #-}
module Haxl.Neo4j.Batch where

import Pipes.HTTP (
    Manager,withHTTP,stream,
    parseUrl,Request(method,requestHeaders,requestBody),
    Response(responseBody))
import Network.HTTP.Types.Header (hAccept,hContentType)

import Pipes.Aeson (decode)
import Pipes.Parse (evalStateT)
import Pipes.Aeson.Unchecked (encode)

import Data.Aeson (
    Value,
    ToJSON(toJSON),object,(.=),
    FromJSON(parseJSON),
    withObject,withText,
    (.:))
import Data.Aeson.Types (Parser)

import Data.HashMap.Strict (HashMap)
import Control.Error (EitherT,runEitherT,left,readErr)
import Data.Text (Text,append,pack,unpack,isPrefixOf)
import qualified  Data.Text as Text (takeWhile,reverse)

import Data.Function (on)

runBatchRequests :: [Neo4jRequest] -> Manager -> IO [Value]
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

data Neo4jRequest =
    NodeById NodeId |
    NodesByLabel Label |
    NodesByLabelAndProperty Label Text Value |
    EdgeById EdgeId |
    IncomingEdges NodeId |
    OutgoingEdges NodeId |
    AllEdges NodeId |
    TypedEdges Label |
    NodeLabels NodeId

deriving instance Show Neo4jRequest

instance ToJSON Neo4jRequest where
    toJSON (NodeById nodeid) = object [
        "method" .= ("GET" :: Text),
        "to" .= nodeURI nodeid]

-- | A neo4j node.
data Node = Node {
    nodeId :: NodeId,
    nodeData :: Properties}

deriving instance Show Node

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

-- | Find a node's URI.
nodeURI :: NodeId -> Text
nodeURI nodeid = "/node/" `append` (pack (show nodeid))

-- | Find an edge's URI.
edgeURI :: EdgeId -> Text
edgeURI edgeid = "/relationship/" `append` (pack (show edgeid))

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
