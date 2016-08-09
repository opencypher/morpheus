package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Column, Dataset, Row}
import org.opencypher.spark.CypherValue
import org.opencypher.spark.impl.{StdField, StdSlot, StdCypherFrame}

object ProductsAsMaps {

  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Map[String, CypherValue]] = {
    new ProductsAsMaps(input)
  }

  class ProductsAsMaps(input: StdCypherFrame[Product]) extends StdCypherFrame[Map[String, CypherValue]](input.signature) {

    val outputMapping = fields.flatMap { (field: StdField)=> signature(field).map(slot => field.columnSym -> slot.ordinal) }

    override def run(implicit context: RuntimeContext): Dataset[Map[String, CypherValue]] = {
      val in = input.run
      val out = in.map(ProductAsMap(outputMapping))(CypherValue.implicits.cypherValueMapEncoder[CypherValue])
      out
    }
  }

  final case class ProductAsMap(slots: Seq[(Symbol, Int)]) extends (Product => Map[String, CypherValue]) {

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
