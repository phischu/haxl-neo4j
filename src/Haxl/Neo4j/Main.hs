{-# LANGUAGE OverloadedStrings #-}
module Main where

import Haxl.Neo4j (runNeo4j,Neo4j,Node,nodesByLabel,typedEdges,Direction(In),edgeSource)

main :: IO ()
main = runNeo4j (usages) >>= print

type DeclarationNode = Node
type SymbolNode = Node

usages :: Neo4j (DeclarationNode,SymbolNode)
usages = do
    symbol <- nodesByLabel "Symbol"
    usingdeclaration <- return symbol >>= typedEdges In "MENTIONEDSYMBOL" >>= edgeSource
    return (usingdeclaration,symbol)
