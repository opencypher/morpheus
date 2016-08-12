package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherValue
import org.opencypher.spark.impl.{ProductFrame, StdCypherFrame, StdRuntimeContext}

object ValueAsProduct {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): ProductFrame = new ValuesAsProducts(input)

  private final class ValuesAsProducts[T <: CypherValue](input: StdCypherFrame[T]) extends ProductFrame(input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(valueAsProduct)(context.productEncoder(slots))
      out
    }
  }

  private case object valueAsProduct extends (CypherValue => Product) {
    override def apply(v: CypherValue): Product = Tuple1(v)
  }
}
