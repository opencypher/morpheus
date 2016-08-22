package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row}
import org.opencypher.spark.impl.StdCypherFrame
import org.opencypher.spark.api.value.CypherValue

object ValueAsRow {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): StdCypherFrame[Row] =
    ValueAsRow(input = input)

  private final case class ValueAsRow[T <: CypherValue](input: StdCypherFrame[T]) extends StdCypherFrame[Row](input.signature) {

    override def execute(implicit context: RuntimeContext): Dataset[Row] = {
      val in = input.run
      val out = in.toDF(slots.head.sym.name)
      out
    }
  }
}
