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
package org.opencypher.spark.api.spark

import cats.data.NonEmptyVector
import org.apache.spark.sql.functions.lit
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.schema.{PropertyKeyMap, Schema}
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.spark.impl.convert.toSparkType
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.record.{InternalHeader, SparkCypherRecordsTokens}
import org.opencypher.spark.impl.spark.SparkColumnName

trait SparkCypherGraph extends CypherGraph with Serializable {

  self =>

  final override type Space = SparkGraphSpace
  final override type Graph = SparkCypherGraph
  final override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(implicit space: SparkGraphSpace): SparkCypherGraph =
    new EmptyGraph() {}

  def create(nodes: NodeScan, scans: GraphScan*)(implicit space: SparkGraphSpace): SparkCypherGraph = {
    val allScans = nodes +: scans

    val schema = allScans.map(_.schema).reduce(_ ++ _)

    // updates the registry associated with the graph space
    space.tokens = SparkCypherRecordsTokens(TokenRegistry.fromSchema(space.tokens.registry, schema))

    new ScanGraph(allScans, schema) {}
  }

  sealed abstract class EmptyGraph(implicit val space: SparkGraphSpace) extends SparkCypherGraph {

    implicit val engine = space.engine

    override def schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))
  }

  // TODO: Header map
  // TODO: CapsContext

  sealed abstract class ScanGraph(val scans: Seq[GraphScan], val schema: Schema)
                                 (implicit val space: SparkGraphSpace) extends SparkCypherGraph {

    // Q: Caching?

    // TODO: Normalize (dh partition away/remove all optional label fields, rel type fields)

    self: SparkCypherGraph =>

    implicit val engine = space.engine

    private val nodeEntityScans = NodeEntityScans(scans.collect { case it: NodeScan => it }.toVector)
    private val relEntityScans = RelationshipEntityScans(scans.collect { case it: RelationshipScan => it }.toVector)

    private def globalRegistry: TokenRegistry = space.tokens.registry

    override def nodes(name: String, nodeCypherType: CTNode) = {

      // TODO: Drop aliases in node scans or here?
      // TODO: Handle empty case

      // (1) find all scans smaller than or equal to the given cypher type if any
      val selectedScans = nodeEntityScans.scans(nodeCypherType)

      // (2) rename scans consistently
      val node = Var(name)(nodeCypherType)
      val tempSchema = selectedScans
        .map(_.schema)
        .reduce(_ ++ _)
      val tempHeader = RecordHeader.nodeFromSchema(node, tempSchema, globalRegistry)

      val selectedRecords = alignEntityVariable(selectedScans, node)

      // (3) Update all non-nullable property types to nullable
      val targetSchema = Schema(tempSchema.labels,
        tempSchema.relationshipTypes,
        PropertyKeyMap.asNullable(tempSchema.nodeKeyMap),
        tempSchema.relKeyMap,
        tempSchema.impliedLabels,
        tempSchema.labelCombinations)

      val targetHeader = RecordHeader.nodeFromSchema(node, targetSchema, globalRegistry)

      // (4) Adjust individual scans to same header
      val alignedRecords = alignRecords(selectedRecords, tempHeader, targetHeader)

      // (5) Union all scan records based on final schema
      SparkCypherRecords.create(targetHeader, alignedRecords.map(_.details.toDF()).reduce(_ union _))
    }

    private def contentExprs(content: Set[SlotContent]) = {
      content.map {
        case OpaqueField(v) => v
        case content: ProjectedSlotContent => content.expr
      }
    }

    override def relationships(name: String, relCypherType: CTRelationship) = {
      // (1) find all scans smaller than or equal to the given cypher type if any
      val selectedScans = relEntityScans.scans(relCypherType)

      // (2) rename scans consistently
      val rel = Var(name)(relCypherType)
      val tempSchema = selectedScans.map(_.schema).reduce(_ ++ _)
      val selectedRecords = alignEntityVariable(selectedScans, rel)
      val tempHeader = RecordHeader.relationshipFromSchema(rel, tempSchema, globalRegistry)

      // (3) Update all non-nullable property types to nullable
      val targetSchema = Schema(tempSchema.labels,
        tempSchema.relationshipTypes,
        tempSchema.nodeKeyMap,
        PropertyKeyMap.asNullable(tempSchema.relKeyMap),
        tempSchema.impliedLabels,
        tempSchema.labelCombinations)

      val targetHeader = RecordHeader.relationshipFromSchema(rel, targetSchema, globalRegistry)

      // (4) Adjust individual scans to same header
      val alignedRecords = alignRecords(selectedRecords, tempHeader, targetHeader)

      // (5) Union all scan records based on final schema
      val data = alignedRecords.map(_.details.toDF()).reduce(_ union _)
      SparkCypherRecords.create(targetHeader, data)
    }

    /**
      * Aligns the given records to the specified target header by adding and re-ordering columns in the associated
      * data frames.
      *
      * @param records records to update
      * @param tempHeader original union header
      * @param targetHeader final union header (including nullable types)
      * @return updates records
      */
    private def alignRecords(records: Seq[SparkCypherRecords],
                             tempHeader: RecordHeader,
                             targetHeader: RecordHeader)
    : Seq[SparkCypherRecords] = {

      records.map { scanRecords =>
        val data = scanRecords.details.toDF()
        val labels = scanRecords.header.slotFor(scanRecords.header.fields.head).content match {
          case o: OpaqueField => o.field.cypherType match {
            case cn: CTNode => cn.labels.filter(p => p._2).keys.toSeq
            case cr: CTRelationship => cr.types.toSeq
          }
        }

        val slotMap = scanRecords.details.header.slots.map(slot => slot.content.key.withoutType -> slot).toMap

        //TODO don't expand common labels, use CTNode label instead
        val newData = tempHeader.slots.foldLeft(data) { (acc, slot) =>
          slotMap.get(slot.content.key.withoutType) match {
            case None =>
              val columnName = SparkColumnName.of(slot)
              slot.content.key match {
                case h: HasLabel =>
                  acc.withColumn(columnName, lit(labels.contains(h.label.name)))
                case h: HasType =>
                  acc.withColumn(columnName, lit(labels.contains(h.relType.name)))
                case t: TypeId =>
                  acc.withColumn(columnName, lit(globalRegistry.relTypeRefByName(labels.head).id))
                case _ => acc.withColumn(columnName, lit(null).cast(toSparkType(slot.content.cypherType.nullable)))
              }
            case _ => acc
          }
        }

        // order dataframe columns according to common header
        val columnNames = targetHeader.slots.map(SparkColumnName.of)

        SparkCypherRecords.create(targetHeader, newData.select(columnNames.head, columnNames.tail: _*))
      }
    }

    /**
      * Updates all given scans to the specified variable.
      *
      * @param selectedScans entity scans
      * @param v common variable
      * @return entity scans with updated headers and data frame columns
      */
    private def alignEntityVariable(selectedScans: Seq[GraphScan], v: Var) = {
      selectedScans.map { scan =>
        val recordSlots = scan.records.details.header.contents
        val renamedRecordSlots: Set[SlotContent] = recordSlots.map {
          case o: OpaqueField => OpaqueField(Var(v.name)(o.cypherType))
          case ProjectedExpr(expr) => expr match {
            case h: HasLabel => ProjectedExpr(HasLabel(v, h.label)(h.cypherType))
            case t: HasType => ProjectedExpr(HasType(v, t.relType)(t.cypherType))
            case p: Property => ProjectedExpr(Property(v, p.key)(p.cypherType))
            case s: StartNode => ProjectedExpr(StartNode(v)(s.cypherType))
            case e: EndNode => ProjectedExpr(EndNode(v)(e.cypherType))
            case _ => Raise.impossible()
          }
        }
        val nameMap = recordSlots.toSeq.sortBy(slot => slot.key.toString)(Ordering.String)
          .zip(renamedRecordSlots.toSeq.sortBy(_.key.toString)(Ordering.String))
          .map(t => SparkColumnName.of(t._1) -> SparkColumnName.of(t._2)).toMap
        val renamedHeader = RecordHeader(InternalHeader(renamedRecordSlots.toSeq: _*))
        val df = scan.records.details.toDF()
        val renamedDF = df.columns.foldLeft(df)((df, col) => df.withColumnRenamed(col, nameMap(col)))

        SparkCypherRecords.create(renamedHeader, renamedDF)
      }
    }

    private case class NodeEntityScans(override val entityScans: Vector[NodeScan]) extends EntityScans {
      override type EntityType = CTNode
      override type EntityScan = NodeScan
    }

    private case class RelationshipEntityScans(override val entityScans: Vector[RelationshipScan]) extends EntityScans {
      override type EntityType = CTRelationship
      override type EntityScan = RelationshipScan
    }

    private trait EntityScans {
      type EntityType <: CypherType with DefiniteCypherType
      type EntityScan <: GraphScan {type EntityCypherType = EntityType}

      def entityScans: Vector[EntityScan]

      lazy val entityScanTypes: Seq[EntityType] = entityScans.map(_.entityType)

      lazy val entityScansByType: Map[EntityType, NonEmptyVector[EntityScan]] =
        entityScans
          .groupBy(_.entityType)
          .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

      def scans(entityType: EntityType) = {
        val candidateTypes = entityScanTypes.filter(_.subTypeOf(entityType).isTrue)
        // TODO: we always select the head of the NonEmptyVector, why not changing to Map[EntityType, EntityScan]?
        // TODO: does this work for relationships?
        val selectedScans = candidateTypes.flatMap(typ => entityScansByType.get(typ).map(_.head))
        selectedScans
      }
    }
  }
}
