package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherValue
import org.opencypher.spark.impl.util.productize
import org.opencypher.spark.impl.{StdCypherFrame, StdRuntimeContext}

object ValueAsProduct {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): ValuesAsProducts[T] = new ValuesAsProducts(input)

  class ValuesAsProducts[T <: CypherValue](input: StdCypherFrame[T]) extends StdCypherFrame[Product](input.signature) {

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val mapped = in.map(valueAsProduct)(context.productEncoder(slots))
      val out = alias(mapped)(context.productEncoder(slots))
      out
    }
  }

  case object valueAsProduct extends (CypherValue => Product) {

    override def apply(v: CypherValue): Product = productize(Seq(v))
  }

}
