package org.opencypher.spark.api.record

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType}

sealed trait GraphScan {

  self =>

  type EntityCypherType <: CypherType

  def records: SparkCypherRecords

  def entityVar = Var(entityName)(entityType)

  def entityName: String
  def entityType: EntityCypherType
}

sealed trait GraphScanFactory[E, S <: GraphScan] {
  def create(entity: E, records: SparkCypherRecords): S
}

object GraphScan {

  implicit object nodeScanFactory extends GraphScanFactory[VerifiedEmbeddedEntity[EmbeddedNode], NodeScan] {
    override def create(entity: VerifiedEmbeddedEntity[EmbeddedNode], records: SparkCypherRecords): NodeScan = {
      val embedded = entity.v
      NodeScan(embedded.entitySlot, embedded.entityType, records)
    }
  }

  implicit object relScanFactory extends GraphScanFactory[VerifiedEmbeddedEntity[EmbeddedRelationship], RelScan] {
    override def create(entity: VerifiedEmbeddedEntity[EmbeddedRelationship], records: SparkCypherRecords): RelScan = {
      val embedded = entity.v
      RelScan(embedded.entitySlot, embedded.entityType, records)
    }
  }
}

final case class NodeScan(entityName: String, entityType: CTNode, records: SparkCypherRecords) extends GraphScan {
  override type EntityCypherType = CTNode
}

final case class RelScan(entityName: String, entityType: CTRelationship, records: SparkCypherRecords)
  extends GraphScan {

  override type EntityCypherType = CTRelationship
}
