package org.opencypher.spark_legacy.api

import java.io.PrintWriter

import org.apache.spark.sql.Row

trait CypherResultContainer {

  def rows: CypherResult[Row]
  def products: CypherResult[Product]
  def records: CypherResult[CypherRecord]

  def show(): Unit = print(new PrintWriter(System.out))
  def print(writer: PrintWriter): Unit
}
