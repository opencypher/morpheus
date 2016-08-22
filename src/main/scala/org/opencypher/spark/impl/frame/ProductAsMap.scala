package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.StdCypherFrame
import org.opencypher.spark.impl.newvalue.CypherValue

object ProductAsMap extends FrameCompanion {

  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Map[String, CypherValue]] = {
    val outputMapping = input.signature.fields.map {
      field => field.sym -> obtain(input.signature.fieldSlot)(field).ordinal
    }
    ProductAsMap(input)(outputMapping)
  }

  private final case class ProductAsMap(input: StdCypherFrame[Product])(outputMapping: Seq[(Symbol, Int)])
    extends StdCypherFrame[Map[String, CypherValue]](input.signature) {

    override def execute(implicit context: RuntimeContext): Dataset[Map[String, CypherValue]] = {
      val in = input.run
      val out = in.map(convert(outputMapping))(context.cypherRecordEncoder)
      out
    }
  }

  private final case class convert(slots: Seq[(Symbol, Int)]) extends (Product => Map[String, CypherValue]) {

    def apply(product: Product) = {
      val values = product.productIterator.toSeq
      val builder = Map.newBuilder[String, CypherValue]
      slots.foreach {
        case (sym, ordinal) => builder += sym.name -> values(ordinal).asInstanceOf[CypherValue]
      }
      builder.result()
    }
  }
}
