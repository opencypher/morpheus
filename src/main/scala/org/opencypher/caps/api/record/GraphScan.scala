/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.record

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.opencypher.caps.api.expr.{HasLabel, OfType, Property, Var}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.{SparkColumn, SparkColumnName}
import org.opencypher.caps.ir.api.Label

sealed trait GraphScan extends Serializable {

  self =>

  type EntityCypherType <: CypherType

  def records: CAPSRecords
  def entity: Var = Var(entityName)(entityType)

  def entityName: String
  def entityType: EntityCypherType

  def schema: Schema
}

object GraphScan extends GraphScanCompanion[EmbeddedEntity] {

  /**
    * Align the argument `CAPSRecords` to the target header and rename the stored entity to `v`.
    *
    * It is required that the `CAPSRecords` instance is a scan, meaning that it must contain exactly a single entity
    * (node or relationship) and its parts (flattened). The stored entity is renamed by this function to the argument
    * variable `v`.
    *
    * @param records the scan to align
    * @param v the variable that the aligned scan should contain
    * @param targetHeader the header to align with
    * @return a new instance of `CAPSRecords` aligned with the argument header
    */
  def align(records: CAPSRecords, v: Var, targetHeader: RecordHeader)
           (implicit session: CAPSSession): CAPSRecords = {
    val oldEntity = records.header.fields.headOption.getOrElse(Raise.impossible("GraphScan table did not contain any fields"))
    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels) => labels
      case CTRelationship(typ) => typ
    }

    val slots = records.details.header.slots
    val renamedSlots = slots.map(_.withOwner(v))

    val dataColumnNameToIndex: Map[String, Int] = renamedSlots.map { dataSlot =>
      val dataColumnName = SparkColumnName.of(dataSlot)
      val dataColumnIndex = dataSlot.index
      dataColumnName -> dataColumnIndex
    }.toMap

    val slotDataSelectors: Seq[Row => Any] = targetHeader.slots.map { targetSlot =>
      val columnName = SparkColumnName.of(targetSlot)
      val defaultValue = targetSlot.content.key match {
        case HasLabel(_, l: Label) => entityLabels(l.name)
        case _: OfType if entityLabels.size == 1 => entityLabels.head
        case _ => null
      }
      val maybeDataIndex = dataColumnNameToIndex.get(columnName)
      val slotDataSelector: Row => Any = maybeDataIndex match {
        case None => (_) => defaultValue
        case Some(index) => _.get(index)
      }
      slotDataSelector
    }

    val alignedData = records.details.toDF().map { (row: Row) =>
      val alignedRow = slotDataSelectors.map(_ (row))
      new GenericRowWithSchema(alignedRow.toArray, targetHeader.asSparkSchema).asInstanceOf[Row]
    }(targetHeader.rowEncoder)

    CAPSRecords.create(targetHeader, alignedData)
  }
}

sealed trait GraphScanCompanion[E <: EmbeddedEntity] {
  def apply[X <: E](verifiedEntity: VerifiedEmbeddedEntity[X]): GraphScanBuilder[X] = GraphScanBuilder(verifiedEntity)
}

sealed trait NodeScan extends GraphScan {
  override type EntityCypherType = CTNode
  override def entityType: CTNode
}

object NodeScan extends GraphScanCompanion[EmbeddedNode] {
  def on(entityAndIdSlot: String)(f: EmbeddedNodeBuilder[(String, String)] => EmbeddedNode)
  : GraphScanBuilder[EmbeddedNode] =
    NodeScan(f(EmbeddedNode(entityAndIdSlot)).verify)

  def on(entitySlotAndIdSlot: (String, String))
        (f: EmbeddedNodeBuilder[(String, String)] => EmbeddedNode)
  : GraphScanBuilder[EmbeddedNode] =
    NodeScan(f(EmbeddedNode(entitySlotAndIdSlot)).verify)
}

sealed trait RelationshipScan extends GraphScan {
  override type EntityCypherType = CTRelationship
}

object RelationshipScan extends GraphScanCompanion[EmbeddedRelationship] {
  def on(entityAndIdSlot: String)
        (f: EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] => EmbeddedRelationship)
  : GraphScanBuilder[EmbeddedRelationship] =
    RelationshipScan(f(EmbeddedRelationship(entityAndIdSlot)).verify)

  def on(entitySlotAndIdSlot: (String, String))
        (f: EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] => EmbeddedRelationship)
  : GraphScanBuilder[EmbeddedRelationship] =
    RelationshipScan(f(EmbeddedRelationship(entitySlotAndIdSlot)).verify)
}

sealed case class GraphScanBuilder[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E])

object GraphScanBuilder {
  sealed abstract class RichGraphScanBuilder[E <: EmbeddedEntity, S <: GraphScan] {
    def builder: GraphScanBuilder[E]

    def from(records: CAPSRecords) = {
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
          CAPSRecords.create(newHeader, newData)(records.caps)
        }
      create(entity, newRecords, schema(entity, newRecords.details.header))
    }

    protected def create(entity: E, records: CAPSRecords, schema: Schema): S

    protected def schema(entity: E, header: RecordHeader): Schema

    protected def getPropertyKeys(entity: EmbeddedEntity, header: RecordHeader): Seq[(String, CypherType)] = {
      val headerKeys = header.slots.map(_.content.key)
      entity.propertiesFromSlots.keys
        .map(key => key -> headerKeys
          .collectFirst { case p: Property if p.m == entity.entityVar && p.key.name == key => p.cypherType }
          .getOrElse(Raise.slotNotFound(s"variable ${entity.entityVar} with key $key when searching in $headerKeys"))
        ).toSeq
    }
  }

  implicit final class RichNodeScanBuilder(val builder: GraphScanBuilder[EmbeddedNode])
    extends RichGraphScanBuilder[EmbeddedNode, NodeScan] {

    override protected def create(scanEntity: EmbeddedNode, scanRecords: CAPSRecords, scanSchema: Schema): NodeScan =
      new NodeScan {
        override def records: CAPSRecords = scanRecords
        override def entityType: CTNode = scanEntity.entityType
        override def entityName: String = scanEntity.entitySlot
        override def schema: Schema = scanSchema
      }

    override protected def schema(entity: EmbeddedNode, header: RecordHeader): Schema = {
      val impliedLabels = entity.labelsFromSlotOrImplied.filterNot(_._2.isDefined).keys

      val impliedPairs = for {
        l <- entity.labelsFromSlotOrImplied.keys
        r <- impliedLabels
      } yield l -> r

      val schemaWithImpliedLabels = impliedPairs
        .foldLeft(Schema.empty)((schema, pair) => schema withImpliedLabel pair)

      val combinations = entity.labelsFromSlotOrImplied.keys.toSeq

      val schemaWithOptionalLabels = schemaWithImpliedLabels.withLabelCombination(combinations: _*)

      val propertyKeys = getPropertyKeys(entity, header)
      impliedLabels.foldLeft(schemaWithOptionalLabels)((schema, label) =>
        schema.withNodePropertyKeys(label)(propertyKeys: _*)
      )
    }
  }

  implicit final class RichRelScanBuilder(val builder: GraphScanBuilder[EmbeddedRelationship])
    extends RichGraphScanBuilder[EmbeddedRelationship, RelationshipScan] {

    override protected def create(scanEntity: EmbeddedRelationship, scanRecords: CAPSRecords, scanSchema: Schema): RelationshipScan =
      new RelationshipScan {
        override def records: CAPSRecords = scanRecords
        override def entityType: CTRelationship = scanEntity.entityType
        override def entityName: String = scanEntity.entitySlot
        override def schema: Schema = scanSchema
      }

    override protected def schema(entity: EmbeddedRelationship, header: RecordHeader): Schema = {
      val relType = entity.relTypeSlotOrName match {
        case Right(name) => name
        case Left((key, _)) => key
      }
      Schema.empty
        .withRelationshipPropertyKeys(relType)(getPropertyKeys(entity, header): _*)
    }
  }
}
