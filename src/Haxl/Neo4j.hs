{-# LANGUAGE GADTs,DeriveDataTypeable,OverloadedStrings,
    FlexibleInstances,MultiParamTypeClasses,StandaloneDeriving,
    TypeFamilies,RebindableSyntax #-}
module Haxl.Neo4j where

import Haxl.Neo4j.Requests (
    Neo4jRequest(..),
    NodeId,Node)

import Haxl.Prelude

import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

import Data.Function (on)

type Haxl = GenHaxl ()

deriving instance Show (Neo4jRequest a)
deriving instance Eq (Neo4jRequest a)

instance DataSource u Neo4jRequest where
    fetch = neo4jFetch

instance Show1 Neo4jRequest where
    show1 = show

instance DataSourceName Neo4jRequest where
    dataSourceName _ = "Neo4j"

instance StateKey Neo4jRequest where
    data State Neo4jRequest = Neo4jState

instance Hashable (Neo4jRequest a) where
    hashWithSalt s (NodeById i) = hashWithSalt s (0::Int,i)

neo4jFetch :: State Neo4jRequest -> Flags -> u -> [BlockedFetch Neo4jRequest] -> PerformFetch
neo4jFetch = asyncFetch
    (\f -> putStrLn "starting" >> f () >> putStrLn "ending")
    (\() -> putStrLn "dispatching")
    (\() -> actuallyFetch)

actuallyFetch :: Neo4jRequest a -> IO (IO (Either SomeException a))
actuallyFetch (NodeById i) = return (return (Right undefined))

gather :: Neo4j a -> Neo4j [a]
gather = Neo4j . (fmap (:[])) . unNeo4j

scatter :: [a] -> Neo4j a
scatter as = Neo4j (return as)

nodeById :: NodeId -> Neo4j Node
nodeById = Neo4j . fmap (:[]) . dataFetch . NodeById

runNeo4j :: Neo4j a -> IO [a]
runNeo4j neo4j = do
    env <- initEnv (stateSet Neo4jState stateEmpty) ()
    runHaxl env (unNeo4j neo4j)

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