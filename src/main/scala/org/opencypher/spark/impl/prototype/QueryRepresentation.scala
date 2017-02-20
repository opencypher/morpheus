package org.opencypher.spark.impl.prototype

import scala.collection.immutable.SortedSet

trait QueryRepresentation[E] {
  def cypherQuery: String
  def cypherVersion: String
  def returns: SortedSet[(Field, String)]
  def params: Map[Param, String]
  def root: RootBlock[E]
}

trait RootBlock[E] {
  def outputs: Set[Field]

  def params: Set[Param]
  def variables: Set[Var]
  def tokens: TokenDefs
  def blocks: BlockStructure[E]

  def solve: BlockDef[E] = blocks(blocks.solve)
}

case class BlockStructure[E](blocks: Map[BlockRef, BlockDef[E]], solve: BlockRef) {
  def apply(ref: BlockRef): BlockDef[E] = blocks(ref)
}

case class QueryRepr[E](cypherQuery: String, returns: SortedSet[(Field, String)], params: Map[Param, String], root: RootBlock[E]) extends QueryRepresentation[E] {
  override val cypherVersion = "Spark Cypher 0.1"
}

case class RootBlockImpl[E](outputs: Set[Field], params: Set[Param], variables: Set[Var], tokens: TokenDefs, blocks: BlockStructure[E]) extends RootBlock[E]
