package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.spark.api.{CypherNode, CypherValue}
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.impl.frame._

class FrameProducer(implicit val planningContext: PlanningContext) {

  def allNodes(sym: Symbol) = AllNodes(sym)
  def allRelationships(sym: Symbol) = AllRelationships(sym)


  implicit final class RichValueFrame[T <: CypherValue](input: StdCypherFrame[T]) {
    def asProduct = ValueAsProduct(input)
    def asRow = ValueAsRow(input)
  }

  implicit final class RichNodeFrame(input: StdCypherFrame[CypherNode]) {
    def labelFilter(labels: String*) = LabelFilterNode(input)(labels)
  }

  implicit final class RichProductFrame(input: StdCypherFrame[Product]) {
    def asRow = ProductAsRow(input)

    def unionAll(other: StdCypherFrame[Product]) =
      UnionAll(input, other)

    def nodeProperty(node: Symbol, propertyKey: Symbol)(outputName: Symbol) =
      GetProperty(input)(node, propertyKey)(outputName -> CTAny.nullable)

    def aliasField(alias: (Symbol, Symbol)) = {
      val (oldName, newName) = alias
      AliasField(input)(oldName)(newName)
    }

    def selectFields(fields: Symbol*) =
      SelectProductFields(input)(fields: _*)

    def relationshipStartId(entity: Symbol)(output: Symbol) =
      ProjectFromEntity.relationshipStartId(input)(entity)(output)

    def relationshipEndId(entity: Symbol)(output: Symbol) =
      ProjectFromEntity.relationshipEndId(input)(entity)(output)

    def nodeId(entity: Symbol)(output: Symbol) =
      ProjectFromEntity.nodeId(input)(entity)(output)

    def relationshipId(entity: Symbol)(output: Symbol) =
      ProjectFromEntity.relationshipId(input)(entity)(output)
  }

  implicit final class RichRowFrame(input: StdCypherFrame[Row]) {
    def asProduct = RowAsProduct(input)
  }
}
