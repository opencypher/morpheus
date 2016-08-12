package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row}
import org.opencypher.spark.impl.StdCypherFrame

object RowAsProduct {

  def apply(input: StdCypherFrame[Row]): StdCypherFrame[Product] =
    new RowAsProduct(input)

  private final class RowAsProduct(input: StdCypherFrame[Row]) extends StdCypherFrame[Product](input.signature) {

    override def execute(implicit context: RuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.as[Product](context.productEncoder(slots))
      out
    }
  }
}


