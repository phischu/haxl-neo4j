{-# LANGUAGE GADTs,DeriveDataTypeable,OverloadedStrings,
    FlexibleInstances,MultiParamTypeClasses,StandaloneDeriving,
    TypeFamilies,RebindableSyntax #-}
module Haxl.Neo4j where

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

gather :: Neo4j a -> Neo4j [a]
gather = Neo4j . (fmap (:[])) . unNeo4j

scatter :: [a] -> Neo4j a
scatter as = Neo4j (return as)

nodeById :: NodeId -> Neo4j Node
nodeById = Neo4j . fmap (:[]) . dataFetch . NodeById

runNeo4j :: Neo4j a -> IO [a]
runNeo4j neo4j = do
    withManager defaultManagerSettings (\manager -> do
        environment <- initEnv (stateSet (Neo4jState manager) stateEmpty) ()
        runHaxl environment (unNeo4j neo4j))

main :: IO ()
main = runNeo4j (nodeById 5) >>= print


newtype Neo4j a = Neo4j {unNeo4j :: Haxl [a]}

instance Monad Neo4j where
    return = Neo4j . return . (:[])
    ma >>= amb = Neo4j (bind (unNeo4j ma) (unNeo4j . amb))

bind :: Haxl [a] -> (a -> Haxl [b]) -> Haxl [b]
bind has ahbs = do
    as <- has
    bss <- forM as ahbs
    return (concat bss)