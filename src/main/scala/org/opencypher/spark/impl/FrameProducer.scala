package org.opencypher.spark.impl

import org.opencypher.spark.api.{CypherNode, CypherValue}
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.impl.frame._

class FrameProducer(implicit val planningContext: PlanningContext) {
  def allNodes(sym: Symbol) = AllNodes(sym)
  def allRelationships(sym: Symbol) = AllRelationships(sym)


  implicit final class RichValueFrame[T <: CypherValue](input: StdCypherFrame[T]) {
    def asProduct = ValueAsProduct(input)
  }

  implicit final class RichNodeFrame(input: StdCypherFrame[CypherNode]) {
    def labelFilter(labels: String*) = LabelFilterNode(input)(labels)
  }

  implicit final class RichProductFrame(input: StdCypherFrame[Product]) {
    def getNodeProperty(node: Symbol, propertyKey: Symbol)(outputName: Symbol) =
      GetProperty(input)(node, propertyKey)(outputName -> CTAny.nullable)

    def aliasField(alias: (Symbol, Symbol)) = {
      val (oldName, newName) = alias
      AliasField(input)(oldName)(newName)
    }

    def projectId(entity: Symbol)(output: Symbol) = {
      ProjectEntityId(input)(entity)(output)
    }
  }
}
