{-# LANGUAGE GADTs,DeriveDataTypeable,OverloadedStrings,
    FlexibleInstances,MultiParamTypeClasses,StandaloneDeriving,
    TypeFamilies,RebindableSyntax #-}
module Haxl.Neo4j where

import Haxl.Prelude

import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

type Haxl = GenHaxl ()
newtype Neo4j a = Neo4j {unNeo4j :: Haxl [a]}

instance Monad Neo4j where
    return = Neo4j . return . (:[])
    ma >>= amb = Neo4j (bind (unNeo4j ma) (unNeo4j . amb))

bind :: Haxl [a] -> (a -> Haxl [b]) -> Haxl [b]
bind has ahbs = do
    as <- has
    bss <- forM as ahbs
    return (concat bss)

data Neo4jReq a where
    Get :: Int -> Neo4jReq [Int]
        deriving Typeable

instance DataSource u Neo4jReq where
    fetch = neo4jFetch

deriving instance Show (Neo4jReq a)
deriving instance Eq (Neo4jReq a)

instance Show1 Neo4jReq where
    show1 = show

instance DataSourceName Neo4jReq where
    dataSourceName _ = "Neo4j"

instance StateKey Neo4jReq where
    data State Neo4jReq = Neo4jState

instance Hashable (Neo4jReq a) where
    hashWithSalt s (Get i) = hashWithSalt s (0::Int,i)

neo4jFetch :: State Neo4jReq -> Flags -> u -> [BlockedFetch Neo4jReq] -> PerformFetch
neo4jFetch = asyncFetch
    (\f -> putStrLn "starting" >> f () >> putStrLn "ending")
    (\() -> putStrLn "dispatching")
    (\() -> actuallyFetch)

actuallyFetch :: Neo4jReq a -> IO (IO (Either SomeException a))
actuallyFetch (Get i) = return (return (Right [i+x | x <- [1..4]]))

gather :: Neo4j a -> Neo4j [a]
gather = Neo4j . (fmap (:[])) . unNeo4j

scatter :: [a] -> Neo4j a
scatter as = Neo4j (return as)

get :: Int -> Neo4j Int
get i = Neo4j (dataFetch (Get i))

runNeo4j :: Neo4j a -> IO [a]
runNeo4j neo4j = do
    env <- initEnv (stateSet Neo4jState stateEmpty) ()
    runHaxl env (unNeo4j neo4j)

main :: IO ()
main = runNeo4j (get 5 >>= get) >>= print

