package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl._

object SelectProductFields {
  def apply(input: StdCypherFrame[Product])(fields: StdField*): SelectProductFields = {
    val (newSignature, slotMapping) = input.signature.selectFields(fields: _*)
    new SelectProductFields(input)(newSignature, slotMapping)
  }

  class SelectProductFields(input: StdCypherFrame[Product])(sig: StdFrameSignature, slots: Seq[StdSlot]) extends StdCypherFrame[Product](sig) {
    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val out = input.run.map(SelectFieldsOfSingleProduct(slots))(context.productEncoder(sig.slots))
      out
    }
  }

  private final case class SelectFieldsOfSingleProduct(slots: Seq[StdSlot]) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val values = product.toVector
      val builder = Vector.newBuilder[Any]
      builder.sizeHint(slots.size)
      slots.foreach { slot => builder += values(slot.ordinal) }
      val newValue = builder.result()
      newValue.toProduct
    }
  }
}
