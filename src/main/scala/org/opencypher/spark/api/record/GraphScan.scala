package org.opencypher.spark.api.record

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.spark.impl.spark.SparkColumn

sealed trait GraphScan extends Serializable {

  self =>

  type EntityCypherType <: CypherType

  def records: SparkCypherRecords
  def entity = Var(entityName)(entityType)

  def entityName: String
  def entityType: EntityCypherType
}

object GraphScan extends GraphScanCompanion[EmbeddedEntity]

sealed trait GraphScanCompanion[E <: EmbeddedEntity] {
  def apply[X <: E](verifiedEntity: VerifiedEmbeddedEntity[X]): GraphScanBuilder[X] = GraphScanBuilder(verifiedEntity)
}

sealed trait NodeScan extends GraphScan {
  override type EntityCypherType = CTNode
}

object NodeScan extends GraphScanCompanion[EmbeddedNode]

sealed trait RelationshipScan extends GraphScan {
  override type EntityCypherType = CTRelationship
}

object RelationshipScan extends GraphScanCompanion[EmbeddedRelationship]

sealed case class GraphScanBuilder[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E])

object GraphScanBuilder {
  sealed abstract class RichGraphScanBuilder[E <: EmbeddedEntity, S <: GraphScan] {
    def builder: GraphScanBuilder[E]

    def from(records: SparkCypherRecords) = {
      val verifiedEntity = builder.entity
      val entity = verifiedEntity.v
      val contracted = records.contract(verifiedEntity)
      val oldSlots = contracted.header.contents
      val newSlots = oldSlots.filter(_.owner.contains(entity.entityVar))
      val newRecords =
        if (newSlots.size == oldSlots.size)
          contracted
        else {
          val newHeader = RecordHeader.from(newSlots.toSeq: _*)
          val newCols = newHeader.slots.map(SparkColumn.from(contracted.data))
          val newData = contracted.data.select(newCols: _*)
          SparkCypherRecords.create(newHeader, newData)(records.space)
        }
      create(entity, newRecords)
    }

    protected def create(entity: E, records: SparkCypherRecords): S
  }

  implicit final class RichNodeScanBuilder(val builder: GraphScanBuilder[EmbeddedNode])
    extends RichGraphScanBuilder[EmbeddedNode, NodeScan] {

    override protected def create(scanEntity: EmbeddedNode, scanRecords: SparkCypherRecords): NodeScan =
      new NodeScan {
        override def records: SparkCypherRecords = scanRecords
        override def entityType: CTNode = scanEntity.entityType
        override def entityName: String = scanEntity.entitySlot
      }
  }

  implicit final class RichRelScanBuilder(val builder: GraphScanBuilder[EmbeddedRelationship])
    extends RichGraphScanBuilder[EmbeddedRelationship, RelationshipScan] {

    override protected def create(scanEntity: EmbeddedRelationship, scanRecords: SparkCypherRecords): RelationshipScan =
      new RelationshipScan {
        override def records: SparkCypherRecords = scanRecords
        override def entityType: CTRelationship = scanEntity.entityType
        override def entityName: String = scanEntity.entitySlot
      }
  }
}
