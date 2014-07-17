{-# LANGUAGE GADTs,DeriveDataTypeable,OverloadedStrings,
    FlexibleInstances,MultiParamTypeClasses,StandaloneDeriving,
    TypeFamilies,RebindableSyntax #-}
module Haxl.Neo4j where

import Haxl.Neo4j.Internal ()

import Pipes.HTTP (Manager,withManager,defaultManagerSettings)

import Data.Aeson (fromJSON,Result(Success),FromJSON,Value)

import Haxl.Prelude

import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

import Data.Function (on)



gather :: Neo4j a -> Neo4j [a]
gather = Neo4j . (fmap (:[])) . unNeo4j

scatter :: [a] -> Neo4j a
scatter as = Neo4j (return as)


runNeo4j :: Haxl a -> IO [a]
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