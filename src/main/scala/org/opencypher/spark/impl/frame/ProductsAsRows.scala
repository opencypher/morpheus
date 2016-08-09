package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Column, Dataset, Row}
import org.opencypher.spark.impl.StdCypherFrame

object ProductsAsRows {
  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Row] =
    new ProductsAsRows(input)

  class ProductsAsRows(input: StdCypherFrame[Product]) extends StdCypherFrame[Row](input.signature) {

    override def run(implicit context: RuntimeContext): Dataset[Row] = {
      val in = input.run
      val columnSymbols = slots.map(_.sym)
      val columns = columnSymbols.zipWithIndex.map {
        case (name, idx) =>
          new Column(s"_${idx + 1}").as(name)
      }
      val out = in.select(columns: _*)
      out
    }
  }
}
