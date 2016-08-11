package org.opencypher.spark.api

import org.apache.spark.sql.Row

trait CypherResultContainer {

  def rows: CypherResult[Row]
  def products: CypherResult[Product]
  def records: CypherResult[CypherRecord]

  def show(): Unit
}
