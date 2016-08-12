package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.CypherNode
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.productize

object PropertyAccessProducts {

  def apply(input: StdCypherFrame[Product], nodeField: StdField, propertyKey: Symbol)
           (outputField: StdField)
           (implicit context: PlanningContext): ProjectFrame = {
    val signature = input.signature.addField(outputField)
    new PropertyAccessProducts(input, nodeField, propertyKey, outputField)(signature)
  }

  private final class PropertyAccessProducts(input: StdCypherFrame[Product], nodeField: StdField, propertyKey: Symbol, outputField: StdField)(sig: StdFrameSignature)
    extends ProjectFrame(outputField, sig) {

    val index = sig(nodeField).getOrElse(throw new IllegalArgumentException("Unknown nodeField")).ordinal

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
