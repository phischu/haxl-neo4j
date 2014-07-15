{-# LANGUAGE StandaloneDeriving,OverloadedStrings,GADTs,DeriveDataTypeable #-}
module Haxl.Neo4j.Requests where

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

import Haxl.Prelude

import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

import Data.Function (on)

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
