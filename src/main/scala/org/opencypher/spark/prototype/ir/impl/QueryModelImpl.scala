package org.opencypher.spark.prototype.ir.impl

import org.opencypher.spark.prototype.Param
import org.opencypher.spark.prototype.ir.{Field, QueryModel, RootBlock}

import scala.collection.immutable.SortedSet

case class QueryModelImpl[E](cypherQuery: String, returns: SortedSet[(Field, String)], params: Map[Param, String], root: RootBlock[E]) extends QueryModel[E] {
  override val cypherVersion = "Spark Cypher 0.1"
}
