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
import org.opencypher.spark.api.expr.{HasLabel, Property, Var}
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.record.InternalHeader
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.spark.operations._
import org.opencypher.spark.impl.syntax.expr._

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

    // TODO: Union
    // TODO: Projection

    override def nodes(name: String, nodeCypherType: CTNode) = {

      // TODO: Drop aliases in node scans or here?
      // TODO: Handle empty case

      // (1) find all scans smaller than or equal to the given cypher type if any
      val selectedScanTypes = nodeEntityScans.entityScanTypes.filter(_.subTypeOf(nodeCypherType).isTrue)
      val selectedScans = selectedScanTypes.map { nodeType => nodeEntityScans.entityScansByType(nodeType).head }

      // (2) rename scans consistently
      val newEntity = Var(name)(nodeCypherType)
      val selectedRecords = selectedScans.map { scan =>

        val recordSlots = scan.records.details.header.contents
        val renamedRecordSlots: Set[SlotContent] = recordSlots.map {
          case o:OpaqueField => OpaqueField(Var(newEntity.name)(o.cypherType))
          case ProjectedExpr(expr) => expr match {
            case p: Property => ProjectedExpr(Property(newEntity, p.key)(p.cypherType))
            case h: HasLabel => ProjectedExpr(HasLabel(newEntity, h.label)(h.cypherType))
            case _ => Raise.impossible()
          }
        }

        val nameMap = recordSlots.zip(renamedRecordSlots).map(t => SparkColumnName.of(t._1) -> SparkColumnName.of(t._2)).toMap
        val renamedHeader = RecordHeader(InternalHeader(renamedRecordSlots.toSeq: _*))
        val df = scan.records.details.toDF()
        val renamedDF = df.columns.foldLeft(df)((df, col) => df.withColumnRenamed(col, nameMap(col)))

        SparkCypherRecords.create(renamedHeader, renamedDF)
      }

      // (3) Compute shared signature
      val selectedContents = selectedRecords.toSet[SparkCypherRecords].map { _.header.contents }
      val sharedContent = selectedContents.reduce(_ intersect _)
      val exclusiveContent = selectedContents.reduce(_ union _) -- sharedContent
      val sharedExprs = contentExprs(sharedContent)

      // (4) Adjust individual scans to same header
      val adjustedRecords = selectedRecords.map { scanRecords =>
        val extraContent = scanRecords.header.contents intersect exclusiveContent
        val extraExprs = contentExprs(extraContent)
        val projectedExprs = sharedExprs ++ extraExprs.map(_.nullable)

        // TODO: Ensure same order between different scans
        // TODO: Select mit slot content
        // self.select(slots)
        self.select(scanRecords, IndexedSeq.empty)
      }

      // (5) Union
      ???
    }

    private def contentExprs(content: Set[SlotContent]) = {
      content.map {
        case OpaqueField(v) => v
        case content: ProjectedSlotContent => content.expr
      }
    }

    override def relationships(name: String, cypherType: CTRelationship) =
      ???

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
        val selectedScans = candidateTypes.flatMap(typ => entityScansByType.get(typ).map(_.head))
        selectedScans
      }
    }
  }
}
