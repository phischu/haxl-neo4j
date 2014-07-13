{-# LANGUAGE GADTs,DeriveDataTypeable,OverloadedStrings,
    FlexibleInstances,MultiParamTypeClasses,StandaloneDeriving,
    TypeFamilies,RebindableSyntax #-}
module Haxl.Neo4j where

import Haxl.Prelude

import Pipes
import Haxl.Core
import Data.Typeable
import Data.Hashable
import Control.Exception

type Haxl = GenHaxl ()

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

get :: Int -> Haxl [Int]
get = dataFetch . Get

main :: IO ()
main = do
    env <- initEnv (stateSet Neo4jState stateEmpty) ()
    res <- runHaxl env (do
        ids <- get 5
        forM ids get)
    print res

