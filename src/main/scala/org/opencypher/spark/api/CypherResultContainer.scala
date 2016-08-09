package org.opencypher.spark.api

import org.apache.spark.sql.Row
import org.opencypher.spark.CypherValue

trait CypherResultContainer {

  def rows: CypherResult[Row]
  def products: CypherResult[Product]
  def maps: CypherResult[Map[String, CypherValue]]

  def show(): Unit
}
