package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.productize
import org.opencypher.spark.{BinaryRepresentation, CypherNode}

object PropertyAccessProducts {

  def apply(input: StdCypherFrame[Product], node: Symbol, propertyKey: Symbol)
           (outputField: StdField)
           (implicit context: PlanningContext): StdCypherFrame[Product] = {
    val outputSlot = StdSlot(context.newSlotSymbol(outputField), outputField.cypherType, BinaryRepresentation)
    new PropertyAccessProducts(input, node, propertyKey)(outputField, outputSlot)
  }

  private class PropertyAccessProducts(input: StdCypherFrame[Product], nodeSym: Symbol, propertyKey: Symbol)(outputField: StdField, outputSlot: StdSlot)
    extends StdCypherFrame[Product](
      _fields = input.fields :+ outputField,
      _slots = input.slots :+ outputSlot
    ) {

    val (_, index) = input.fields.zipWithIndex.find {
      case (f, _) => f.sym == nodeSym
    }.get

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(ProductPropertyAccess(index, propertyKey.name))(context.productEncoder(slots))
      out
    }
  }

  private final case class ProductPropertyAccess(index: Int, propertyKeyName: String) extends (Product => Product) {
    def apply(product: Product): Product = {
      val elts = product.productIterator.toVector
      val node = elts(index).asInstanceOf[CypherNode]
      val value = node.properties.get(propertyKeyName).orNull
      val result = productize(elts :+ value)
      result
    }
  }
}
