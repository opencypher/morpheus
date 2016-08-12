package org.opencypher.spark.impl

import org.opencypher.spark.api.CypherValue
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.impl.frame.{AllNodes, AllRelationships, GetNodeProperty, ValueAsProduct}

class FrameProducer(implicit val planningContext: PlanningContext) {
  def allNodes(sym: Symbol) = AllNodes(sym)
  def allRelationships(sym: Symbol) = AllRelationships(sym)

  def valuesAsProduct[T <: CypherValue](input: StdCypherFrame[T]) = {
    ValueAsProduct(input)
  }

  def getNodeProperty(input: StdCypherFrame[Product])(nodeField: StdField, propertyKey: Symbol)(outputName: Symbol) = {
    GetNodeProperty(input, nodeField, propertyKey)(StdField(outputName, CTAny.nullable))
  }
}
