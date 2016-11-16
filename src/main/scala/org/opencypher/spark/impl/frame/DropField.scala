package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.{ProductFrame, StdCypherFrame, StdFrameSignature, StdRuntimeContext}

object DropField extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(field: Symbol): StdCypherFrame[Product] =  {
    val sig = input.signature.dropField(field)
    val slot = obtain(input.signature.slot)(field)

    DropField(input)(slot.ordinal)(sig)
  }

  private case class DropField(input: StdCypherFrame[Product])(index: Int)(sig: StdFrameSignature) extends ProductFrame(sig) {
    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      in.map(reduceProduct(index))(context.productEncoder(slots))
    }
  }

  private case class reduceProduct(index: Int) extends (Product => Product) {
    import org.opencypher.spark.impl.util._

    override def apply(record: Product): Product = {
      record.drop(index)
    }
  }

}
