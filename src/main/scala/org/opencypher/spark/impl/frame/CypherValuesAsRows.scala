package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row}
import org.opencypher.spark.CypherValue
import org.opencypher.spark.impl.StdCypherFrame

object CypherValuesAsRows {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): StdCypherFrame[Row] =
    new CypherValuesAsRows(input = input)

  class CypherValuesAsRows[T <: CypherValue](input: StdCypherFrame[T]) extends StdCypherFrame[Row](input.signature) {

    override def run(implicit context: RuntimeContext): Dataset[Row] = {
      val in = input.run
      val out = in.toDF(slots.head.sym.name)
      out
    }
  }
}
