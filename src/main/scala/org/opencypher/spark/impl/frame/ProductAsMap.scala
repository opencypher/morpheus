package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Column, Dataset, Row}
import org.opencypher.spark.api.CypherValue
import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdSlot}

object ProductAsMap {

  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Map[String, CypherValue]] = {
    ProductAsMap(input)
  }

  private final case class ProductAsMap(input: StdCypherFrame[Product]) extends StdCypherFrame[Map[String, CypherValue]](input.signature) {

    val outputMapping = fields.map { field => field.sym -> signature.slot(field).ordinal }

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
