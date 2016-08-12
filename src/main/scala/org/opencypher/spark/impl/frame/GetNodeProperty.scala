package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherNode
import org.opencypher.spark.impl._

object GetNodeProperty {

  def apply(input: StdCypherFrame[Product], nodeField: StdField, propertyKey: Symbol)
           (outputField: StdField)
           (implicit context: PlanningContext): ProjectFrame = {
    val signature = input.signature.addField(outputField)
    new PropertyAccessProducts(input, nodeField, propertyKey, outputField)(signature)
  }

  private final class PropertyAccessProducts(input: StdCypherFrame[Product], nodeField: StdField, propertyKey: Symbol, outputField: StdField)(sig: StdFrameSignature)
    extends ProjectFrame(outputField, sig) {

    val index = sig.slot(nodeField).getOrElse(throw new IllegalArgumentException("Unknown nodeField")).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(GetNodeProperty(index, propertyKey.name))(context.productEncoder(slots))
      out
    }
  }

  private final case class GetNodeProperty(index: Int, propertyKeyName: String) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val node = product.getAs[CypherNode](index)
      val value = node.properties.get(propertyKeyName).orNull
      val result = product :+ value
      result
    }
  }
}
