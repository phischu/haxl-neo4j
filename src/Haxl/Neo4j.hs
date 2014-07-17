module Haxl.Neo4j where

import Haxl.Neo4j.Internal (runHaxlNeo4j,nodeById)





main :: IO ()
main = runHaxlNeo4j (nodeById 5) >>= print

