package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.impl.{ProductFrame, StdCypherFrame, StdRuntimeContext}

object ValueAsProduct extends FrameCompanion {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): ProductFrame =
    ValueAsProduct(input)

  private final case class ValueAsProduct[T <: CypherValue](input: StdCypherFrame[T]) extends ProductFrame(input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(convert)(context.productEncoder(slots))
      out
    }
  }

  private case object convert extends (CypherValue => Product) {
    override def apply(v: CypherValue): Product = Tuple1(v)
  }
}
