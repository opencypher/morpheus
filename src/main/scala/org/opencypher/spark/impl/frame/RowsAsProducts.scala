package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row}
import org.opencypher.spark.impl.StdCypherFrame

object RowsAsProducts {

  def apply(input: StdCypherFrame[Row]): StdCypherFrame[Product] =
    new RowsAsProducts(input)

  class RowsAsProducts(input: StdCypherFrame[Row]) extends StdCypherFrame[Product](
    _fields = input.fields,
    _slots = input.slots
  ) {

    override def run(implicit context: RuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.as[Product](context.productEncoder(slots))
      out
    }
  }
}


