package org.opencypher.spark.impl.prototype

import scala.collection.immutable.SortedSet

trait QueryRepresentation {
  def cypherQuery: String
  def cypherVersion: String
  def returns: SortedSet[(Field, String)]
  def params: Map[Param, String]
  def root: RootBlock
}

trait RootBlock {
  def outputs: Set[Field]

  def params: Set[Param]
  def variables: Set[Var]
  def tokens: TokenDefs
  def blocks: BlockStructure

  def solve: BlockRef = blocks.solve
}

case class BlockStructure(blocks: Map[BlockRef, BlockDef], solve: BlockRef)
