package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherAnyMap
import org.opencypher.spark.impl._

object GetProperty {

  def apply(input: StdCypherFrame[Product])(properties: Symbol, propertyKey: Symbol)(outputField: StdField)
           (implicit context: PlanningContext): ProjectFrame = {
    val propertiesField = input.signature.field(properties)
    val signature = input.signature.addField(outputField)
    new GetProperty(input)(propertiesField, propertyKey, outputField)(signature)
  }

  private final class GetProperty(input: StdCypherFrame[Product])
                                 (nodeField: StdField, propertyKey: Symbol, outputField: StdField)
                                 (sig: StdFrameSignature)
    extends ProjectFrame(outputField, sig) {

    val index = sig.slot(nodeField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(getMapProperty(index, propertyKey.name))(context.productEncoder(slots))
      out
    }
  }

  private final case class getMapProperty(index: Int, propertyKeyName: String) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val properties = product.getAs[CypherAnyMap](index)
      val value = properties.properties.get(propertyKeyName).orNull
      val result = product :+ value
      result
    }
  }
}
