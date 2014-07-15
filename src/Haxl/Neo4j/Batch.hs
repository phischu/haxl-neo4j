{-# LANGUAGE StandaloneDeriving,OverloadedStrings,GADTs #-}
module Haxl.Neo4j.Batch where

import Haxl.Neo4j.Requests (
    Neo4jRequest(..),
    NodeId,EdgeId)

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
import Control.Error (readErr)
import Data.Text (Text,append,pack,unpack)
import qualified  Data.Text as Text (takeWhile,reverse)

import Data.Function (on)

runBatchRequests :: [Neo4jRequest a] -> Manager -> IO [Value]
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

instance ToJSON (Neo4jRequest a) where
    toJSON (NodeById nodeid) = object [
        "method" .= ("GET" :: Text),
        "to" .= nodeURI nodeid]

-- | Find a node's URI.
nodeURI :: NodeId -> Text
nodeURI nodeid = "/node/" `append` (pack (show nodeid))

-- | Find an edge's URI.
edgeURI :: EdgeId -> Text
edgeURI edgeid = "/relationship/" `append` (pack (show edgeid))
