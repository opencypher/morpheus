package org.opencypher.spark.api.spark

import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens

// Lightweight wrapper around token registry to expose a simple lookup api for all tokens that may occur in a data frame
trait SparkCypherTokens {

  self: Serializable =>

  type Tokens <: SparkCypherTokens

  def labels: Set[String]
  def relTypes: Set[String]

  def labelName(id: Int): String
  def labelId(name: String): Int

  def relTypeName(id: Int): String
  def relTypeId(name: String): Int

  def withLabel(name: String): Tokens
  def withRelType(name: String): Tokens
}


object SparkCypherTokens {
  val empty = SparkCypherRecordsTokens(TokenRegistry.empty)
}
