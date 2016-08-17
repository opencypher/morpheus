package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Column, Dataset, Row}
import org.opencypher.spark.impl.StdCypherFrame

object ProductAsRow extends FrameCompanion {

  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Row] =
    ProductAsRow(input)

  private final case class ProductAsRow(input: StdCypherFrame[Product]) extends StdCypherFrame[Row](input.signature) {

    override def execute(implicit context: RuntimeContext): Dataset[Row] = {
      val in = input.run
      val columnNames = slots.map(_.sym.name)
      val out = in.toDF(columnNames: _*)
      out
    }
  }
}
