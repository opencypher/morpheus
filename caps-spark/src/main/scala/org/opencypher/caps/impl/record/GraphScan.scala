/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.record

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.IllegalStateException
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.caps.impl.spark.{CAPSRecords, SparkColumn}
import org.opencypher.caps.ir.api.expr._

import scala.language.implicitConversions

sealed trait GraphScan extends Serializable {

  self =>

  type EntityCypherType <: CypherType

  def records: CAPSRecords
  def entity: Var = Var(entityName)(entityType)

  def cache(): GraphScan
  def persist(): GraphScan
  def persist(storageLevel: StorageLevel): GraphScan
  def unpersist(): GraphScan
  def unpersist(blocking: Boolean): GraphScan

  def entityName: String
  def entityType: EntityCypherType

  def schema: Schema
}

object GraphScan extends GraphScanCompanion[EmbeddedEntity] {
  //  implicit def nodesToScan[E <: Node: TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): NodeScan = {
  //    NodeScan(nodes)
  //  }
  //
  //  implicit def relationshipsToScan[E <: Relationship: TypeTag](relationship: Seq[E])(
  //      implicit caps: CAPSSession): RelationshipScan = {
  //    RelationshipScan(relationship)
  //  }


}

sealed trait GraphScanCompanion[E <: EmbeddedEntity] {
  def apply[X <: E](verifiedEntity: VerifiedEmbeddedEntity[X]): GraphScanBuilder[X] = GraphScanBuilder(verifiedEntity)
}

sealed trait NodeScan extends GraphScan {
  override type EntityCypherType = CTNode
  override def entityType: CTNode
}

//object NodeScan extends GraphScanCompanion[EmbeddedNode] {
//  private val nodeIdColumnName = "id"
//
//  private def properties(nodeColumnNames: Seq[String]): Seq[String] = {
//    nodeColumnNames.filter(_ != nodeIdColumnName)
//  }
//
//  def apply[E <: Node: TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): NodeScan = {
//    val nodeLabel = Annotation.labels[E]
//    val nodeRecords = CAPSRecords.create(nodes)
//    val nodeProperties = properties(nodeRecords.sparkColumns)
//
//    NodeScan
//      .on(nodeIdColumnName) { builder =>
//        val withLabels = nodeLabels.foldLeft(builder.build) {
//          case (schema, label) =>
//            schema.withImpliedLabel(label)
//        }
//        nodeProperties.foldLeft(withLabels) {
//          case (schema, nodeProperty) =>
//            schema.withPropertyKey(nodeProperty)
//        }
//      }
//      .from(nodeRecords)
//  }
//
//  def on(entityAndIdSlot: String)(
//      f: EmbeddedNodeBuilder[(String, String)] => EmbeddedNode): GraphScanBuilder[EmbeddedNode] =
//    NodeScan(f(EmbeddedNode(entityAndIdSlot)).verify)
//
//  def on(entitySlotAndIdSlot: (String, String))(
//      f: EmbeddedNodeBuilder[(String, String)] => EmbeddedNode): GraphScanBuilder[EmbeddedNode] =
//    NodeScan(f(EmbeddedNode(entitySlotAndIdSlot)).verify)
//}

sealed trait RelationshipScan extends GraphScan {
  override type EntityCypherType = CTRelationship
}

//object RelationshipScan extends GraphScanCompanion[EmbeddedRelationship] {
//  private val relationshipIdColumnName = "id"
//  private val relationshipSourceColumnName = "source"
//  private val relationshipTargetColumnName = "target"
//  private val nonPropertyAttributes =
//    Set(relationshipIdColumnName, relationshipSourceColumnName, relationshipTargetColumnName)
//
//  private def properties(relationshipRecords: CAPSRecords): Seq[String] = {
//    val columnNames = relationshipRecords.sparkColumns
//    columnNames.filter(!nonPropertyAttributes.contains(_))
//  }
//
//  def apply[E <: Relationship: TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): RelationshipScan = {
//    val relationshipType: String = Annotation.relType[E]
//    val relationshipRecords = CAPSRecords.create(relationships)
//    val relationshipProperties = properties(relationshipRecords)
//
//    RelationshipScan
//      .on(relationshipIdColumnName) { builder =>
//        relationshipProperties.foldLeft(
//          builder.from(relationshipSourceColumnName).to(relationshipTargetColumnName).relType(relationshipType).build
//        ) {
//          case (schema, property) =>
//            schema.withPropertyKey(property)
//        }
//      }
//      .from(relationshipRecords)
//  }
//
//  def on(entityAndIdSlot: String)(
//      f: EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] => EmbeddedRelationship)
//    : GraphScanBuilder[EmbeddedRelationship] =
//    RelationshipScan(f(EmbeddedRelationship(entityAndIdSlot)).verify)
//
//  def on(entitySlotAndIdSlot: (String, String))(
//      f: EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] => EmbeddedRelationship)
//    : GraphScanBuilder[EmbeddedRelationship] =
//    RelationshipScan(f(EmbeddedRelationship(entitySlotAndIdSlot)).verify)
//}

sealed case class GraphScanBuilder[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E])

object GraphScanBuilder {
  sealed abstract class RichGraphScanBuilder[E <: EmbeddedEntity, S <: GraphScan] {
    def builder: GraphScanBuilder[E]

    def fromDataFrame(df: DataFrame)(implicit caps: CAPSSession) = {
      val record = CAPSRecords.prepareDataFrame(df)
      from(record)
    }

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
      create(entity, newRecords, schema(entity, newRecords.header))
    }

    protected def create(entity: E, records: CAPSRecords, schema: Schema): S

    protected def schema(entity: E, header: RecordHeader): Schema

    protected def getPropertyKeys(entity: EmbeddedEntity, header: RecordHeader): Seq[(String, CypherType)] = {
      val headerKeys = header.slots.map(_.content.key)
      entity.propertiesFromSlots.keys
        .map(
          key =>
            key -> headerKeys.collectFirst {
              case p: Property if p.m == entity.entityVar && p.key.name == key => p.cypherType
            }.getOrElse(throw IllegalStateException(
              s"Slot not found using variable ${entity.entityVar} with key $key when searching in $headerKeys")))
        .toSeq
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

        override def cache(): NodeScan = create(scanEntity, scanRecords.cache(), scanSchema)
        override def persist(): NodeScan = create(scanEntity, scanRecords.persist(), scanSchema)
        override def persist(storageLevel: StorageLevel): NodeScan =
          create(scanEntity, scanRecords.persist(storageLevel), scanSchema)
        override def unpersist(): NodeScan = create(scanEntity, scanRecords.unpersist(), scanSchema)
        override def unpersist(blocking: Boolean): NodeScan =
          create(scanEntity, scanRecords.unpersist(blocking), scanSchema)
      }

    override protected def schema(entity: EmbeddedNode, header: RecordHeader): Schema = {
      val impliedLabels = entity.labelsFromSlotOrImplied.filterNot(_._2.isDefined).keySet
      val optionalLabels = entity.labelsFromSlotOrImplied.keySet -- impliedLabels

      val propertyKeys = getPropertyKeys(entity, header)

      optionalLabels.subsets
        .map(_.union(impliedLabels))
        .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
        .reduce(_ ++ _)
    }
  }

  implicit final class RichRelScanBuilder(val builder: GraphScanBuilder[EmbeddedRelationship])
      extends RichGraphScanBuilder[EmbeddedRelationship, RelationshipScan] {

    override protected def create(
        scanEntity: EmbeddedRelationship,
        scanRecords: CAPSRecords,
        scanSchema: Schema): RelationshipScan =
      new RelationshipScan {
        override def records: CAPSRecords = scanRecords
        override def entityType: CTRelationship = scanEntity.entityType
        override def entityName: String = scanEntity.entitySlot
        override def schema: Schema = scanSchema

        override def cache(): RelationshipScan = create(scanEntity, scanRecords.cache(), scanSchema)
        override def persist(): RelationshipScan = create(scanEntity, scanRecords.persist(), scanSchema)
        override def persist(storageLevel: StorageLevel): RelationshipScan =
          create(scanEntity, scanRecords.persist(storageLevel), scanSchema)
        override def unpersist(): RelationshipScan = create(scanEntity, scanRecords.unpersist(), scanSchema)
        override def unpersist(blocking: Boolean): RelationshipScan =
          create(scanEntity, scanRecords.unpersist(blocking), scanSchema)
      }

    override protected def schema(entity: EmbeddedRelationship, header: RecordHeader): Schema = {
      val relType = entity.relTypeSlotOrName match {
        case Right(name)    => name
        case Left((key, _)) => key
      }
      Schema.empty
        .withRelationshipPropertyKeys(relType)(getPropertyKeys(entity, header): _*)
    }
  }
}
