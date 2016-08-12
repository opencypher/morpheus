package org.opencypher.spark.impl

import org.opencypher.spark.api.CypherValue
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.impl.frame.{AllNodes, AllRelationships, GetNodeProperty, ValueAsProduct}

class FrameProducer(implicit val planningContext: PlanningContext) {
  def allNodes(sym: Symbol) = AllNodes(sym)
  def allRelationships(sym: Symbol) = AllRelationships(sym)


  implicit final class RichValueFrame[T <: CypherValue](input: StdCypherFrame[T]) {
    def valuesAsProduct = ValueAsProduct(input)
  }

  implicit final class RichProductFrame(input: StdCypherFrame[Product]) {
    def getNodeProperty(node: Symbol, propertyKey: Symbol)(outputName: Symbol) =
      GetNodeProperty(input, node, propertyKey)(StdField(outputName, CTAny.nullable))
  }
}
