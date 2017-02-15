package org.opencypher.spark.impl.prototype

import org.opencypher.spark.api.CypherType

import scala.collection.immutable.SortedSet

trait QueryRepresentation {
  def cypherQuery: String
  def cypherVersion: String
  def returns: SortedSet[(String, CypherType)]
  def root: RootBlock
}

trait RootBlock {
  def fields: Set[Field]
  def tokens: TokenDefs
  def blocks: Map[BlockRef, BlockDef]

  def solve: BlockRef
}
