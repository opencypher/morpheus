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
package org.opencypher.caps.api.spark

import cats.data.NonEmptyVector
import org.apache.spark.sql.functions.lit
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.graph.CypherGraph
import org.opencypher.caps.ir.api.global.TokenRegistry
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.{PropertyKeyMap, Schema}
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.impl.record.{CAPSRecordsTokens, InternalHeader}
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.exception.Raise

trait CAPSGraph extends CypherGraph with Serializable {

  self =>

  final override type Graph = CAPSGraph
  final override type Records = CAPSRecords
  final override type Session = CAPSSession
  final override type Result = CAPSResult

  def tokens: CAPSRecordsTokens
}

object CAPSGraph {

  def empty(implicit caps: CAPSSession): CAPSGraph =
    new EmptyGraph() {
      override protected def graph = this
      override def session = caps
      override val tokens = CAPSRecordsTokens(TokenRegistry.empty)
    }

  def create(nodes: NodeScan, scans: GraphScan*)(implicit caps: CAPSSession): CAPSGraph = {
    val allScans = nodes +: scans
    val schema = allScans.map(_.schema).reduce(_ ++ _)

    new ScanGraph(allScans, schema) {
      override protected def graph = this
      override val session = caps
      override val tokens = CAPSRecordsTokens(TokenRegistry.fromSchema(schema))
    }
  }

  def createLazy(theSchema: Schema)(loadGraph: => CAPSGraph)(implicit caps: CAPSSession) = new CAPSGraph {
    override protected lazy val graph: CAPSGraph = {
      val g = loadGraph
      if (g.schema == theSchema) g else Raise.schemaMismatch()
    }

    override def tokens: CAPSRecordsTokens = graph.tokens
    override def session: CAPSSession = caps

    override def schema: Schema = theSchema

    override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords =
      graph.nodes(name, nodeCypherType)

    override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords =
      graph.relationships(name, relCypherType)

    override def union(other: CAPSGraph): CAPSGraph =
      graph.union(other)
  }

  sealed abstract class EmptyGraph(implicit val caps: CAPSSession) extends CAPSGraph {

    override val schema = Schema.empty
    override val tokens = CAPSRecordsTokens(TokenRegistry.fromSchema(schema))

    override def nodes(name: String, cypherType: CTNode) =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship) =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def union(other: CAPSGraph): CAPSGraph = other
  }

  sealed abstract class ScanGraph(val scans: Seq[GraphScan], val schema: Schema)
                                 (implicit val session: CAPSSession) extends CAPSGraph {

    // TODO: Caching?

    // TODO: Normalize (ie partition away/remove all optional label fields, rel type fields)
    // TODO: Drop aliases in node scans or here?

    self: CAPSGraph =>

    private val nodeEntityScans = NodeEntityScans(scans.collect { case it: NodeScan => it }.toVector)
    private val relEntityScans = RelationshipEntityScans(scans.collect { case it: RelationshipScan => it }.toVector)

    override def nodes(name: String, nodeCypherType: CTNode) = {

      // (1) find all scans smaller than or equal to the given cypher type if any
      val selectedScans = nodeEntityScans.scans(nodeCypherType)

      // (2) rename scans consistently
      val node = Var(name)(nodeCypherType)
      val tempSchema = selectedScans
        .map(_.schema)
        .reduce(_ ++ _)
      val tempHeader = RecordHeader.nodeFromSchema(node, tempSchema, tokens.registry)

      val selectedRecords = alignEntityVariable(selectedScans, node)

      // (3) Update all non-nullable property types to nullable
      val targetSchema = Schema(tempSchema.labels,
        tempSchema.relationshipTypes,
        PropertyKeyMap.asNullable(tempSchema.nodeKeyMap),
        tempSchema.relKeyMap,
        tempSchema.impliedLabels,
        tempSchema.labelCombinations)
      val targetHeader = RecordHeader.nodeFromSchema(node, targetSchema, tokens.registry)

      // (4) Adjust individual scans to same header
      val alignedRecords = alignRecords(selectedRecords, tempHeader, targetHeader)

      // (5) Union all scan records based on final schema
      CAPSRecords.create(targetHeader, alignedRecords.map(_.details.toDF()).reduce(_ union _))
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
      val tempHeader = RecordHeader.relationshipFromSchema(rel, tempSchema, tokens.registry)

      // (3) Update all non-nullable property types to nullable
      val targetSchema = Schema(tempSchema.labels,
        tempSchema.relationshipTypes,
        tempSchema.nodeKeyMap,
        PropertyKeyMap.asNullable(tempSchema.relKeyMap),
        tempSchema.impliedLabels,
        tempSchema.labelCombinations)
      val targetHeader = RecordHeader.relationshipFromSchema(rel, targetSchema, tokens.registry)

      // (4) Adjust individual scans to same header
      val alignedRecords = alignRecords(selectedRecords, tempHeader, targetHeader)

      // (5) Union all scan records based on final schema
      val data = alignedRecords.map(_.details.toDF()).reduce(_ union _)
      CAPSRecords.create(targetHeader, data)
    }

    override def union(other: CAPSGraph): CAPSGraph = ???

    /**
      * Aligns the given records to the specified target header by adding and re-ordering columns in the associated
      * data frames.
      *
      * @param records records to update
      * @param tempHeader original union header
      * @param targetHeader final union header (including nullable types)
      * @return updates records
      */
    private def alignRecords(records: Seq[CAPSRecords],
                             tempHeader: RecordHeader,
                             targetHeader: RecordHeader)
    : Seq[CAPSRecords] = {

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
                  acc.withColumn(columnName, lit(tokens.relTypeId(labels.head)))
                case _ => acc.withColumn(columnName, lit(null).cast(toSparkType(slot.content.cypherType.nullable)))
              }
            case _ => acc
          }
        }

        // order dataframe columns according to common header
        val columnNames = targetHeader.slots.map(SparkColumnName.of)

        CAPSRecords.create(targetHeader, newData.select(columnNames.head, columnNames.tail: _*))
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

        CAPSRecords.create(renamedHeader, renamedDF)
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
