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
import cats.kernel.Monoid
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.spark.impl.instances.map._

trait SparkCypherGraph extends CypherGraph with Serializable {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(implicit space: SparkGraphSpace): SparkCypherGraph =
    new EmptyGraph() {}

  def create(nodes: NodeScan, scans: GraphScan*)(implicit space: SparkGraphSpace): SparkCypherGraph = {
    val allScans = nodes +: scans
    val schema = ???
    new ScanGraph(allScans, schema) {}
  }

  sealed abstract class EmptyGraph(implicit val space: SparkGraphSpace) extends SparkCypherGraph {
    override def schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))
  }

  sealed abstract class ScanGraph(val scans: Seq[GraphScan], val schema: Schema)
                                 (implicit val space: SparkGraphSpace) extends SparkCypherGraph {

    private val nodeEntityScans = NodeEntityScans(scans.collect { case it: NodeScan => it }.toVector)
    private val relEntityScans = RelationshipEntityScans(scans.collect { case it: RelationshipScan => it }.toVector)

//
//    private val nodeScansByType: Map[CTNode, NonEmptyVector[NodeScan]] =
//      Monoid[Map[CTNode, NonEmptyVector[NodeScan]]]
//        .combineAll(nodeScans.map { it => Map(it.entityType -> NonEmptyVector.of(it)) })
//        .mapValues(_.distinct)
//
//    private val nodeScanTypes: Set[CTNode] = nodeScansByType.keySet
//
//    private val relScans = scans.collect { case it: RelationshipScan => it }

    override def nodes(name: String, cypherType: CTNode) = {
//      // find all scans smaller than or equal to the given cypher type if any
//      val subScanTypes = nodeScanTypes.foldLeft(Set.empty[CTNode]) {
//        case (acc, typ) if typ.subTypeOf(cypherType).maybeTrue =>
//          acc.filter(_.subTypeOf(typ).isTrue) + typ
//      }
//
//
//      val subScans = subScanTypes.map(nodeScansByType).map(_.head)
//
//      if (subScans.isEmpty) {
//
//      }
//      else {
//        // compute union plan
//      }
      ???
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
