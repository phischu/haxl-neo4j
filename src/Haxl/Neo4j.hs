{-# LANGUAGE DeriveFunctor,OverloadedStrings #-}
module Haxl.Neo4j where

import Haxl.Neo4j.Internal (
    runHaxlNeo4j,Haxl,
    NodeId,Node,EdgeId,Edge,Label,Properties,Direction(All))
import qualified Haxl.Neo4j.Internal as Internal (
    nodeById,nodesByLabel,nodeLabels,nodeId,nodeProperties,
    edges,typedEdges,edgeById,
    edgeId,edgeLabel,edgeProperties,edgeSource,edgeTarget)

import Data.Aeson (FromJSON,parseJSON)
import Data.Aeson.Types (parseMaybe)
import Data.Text (Text)
import qualified Data.HashMap.Lazy as HashMap (lookup)

import Control.Monad ((>=>))
import Data.Traversable (traverse)

newtype Neo4j a = Neo4j { unNeo4j :: Haxl [a] }
    deriving (Functor)

runNeo4j :: Neo4j a -> IO [a]
runNeo4j = runHaxlNeo4j . unNeo4j


nodeById :: NodeId -> Neo4j Node
nodeById = Neo4j . fmap (:[]) . Internal.nodeById

nodesByLabel :: Label -> Neo4j Node
nodesByLabel = Neo4j . Internal.nodesByLabel

nodeId :: Node -> Neo4j NodeId
nodeId = Neo4j . return . (:[]) . Internal.nodeId

nodeLabels :: Node -> Neo4j Label
nodeLabels = Neo4j . Internal.nodeLabels . Internal.nodeId

nodeProperties :: Node -> Neo4j Properties
nodeProperties = Neo4j . return . (:[]) . Internal.nodeProperties

nodeProperty :: (FromJSON a) => Text -> Node -> Neo4j a
nodeProperty key = Neo4j . return . maybe [] (:[]) . (HashMap.lookup key >=> parseMaybe parseJSON) . Internal.nodeProperties

edges :: Direction -> Node -> Neo4j Edge
edges direction = Neo4j . Internal.edges direction . Internal.nodeId

typedEdges :: Direction -> Label -> Node -> Neo4j Edge
typedEdges direction label = Neo4j . Internal.typedEdges direction label . Internal.nodeId


edgeById :: EdgeId -> Neo4j Edge
edgeById = Neo4j . fmap (:[]) . Internal.edgeById

edgeId :: Edge -> Neo4j EdgeId
edgeId = Neo4j . return . (:[]) . Internal.edgeId

edgeLabel :: Edge -> Neo4j Label
edgeLabel = Neo4j . return . (:[]) . Internal.edgeLabel

edgeProperties :: Edge -> Neo4j Properties
edgeProperties = Neo4j . return . (:[]) . Internal.edgeProperties

edgeProperty :: (FromJSON a) => Text -> Edge -> Neo4j a
edgeProperty key = Neo4j . return . maybe [] (:[]) . (HashMap.lookup key >=> parseMaybe parseJSON) . Internal.edgeProperties

edgeSource :: Edge -> Neo4j Node
edgeSource = nodeById . Internal.edgeSource

edgeTarget :: Edge -> Neo4j Node
edgeTarget = nodeById . Internal.edgeTarget


instance Monad Neo4j where
    return = Neo4j . return . (:[])
    ma >>= amb = Neo4j (do
        as <- unNeo4j ma
        bss <- traverse (unNeo4j . amb) as
        return (concat bss))


main :: IO ()
main = runNeo4j (nodeById 5 >>= edges All) >>= print

