package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype._

import scala.collection.immutable.SortedSet
trait QueryModel[E] {
  def cypherQuery: String
  def cypherVersion: String
  def returns: SortedSet[(Field, String)]
  def params: Map[Param, String]
  def root: RootBlock[E]
}

final case class Field(name: String) extends AnyVal

