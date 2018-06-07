/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Expr._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.util.TagSupport.computeRetaggings
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

object CAPSUnionGraph {
  def apply(graphs: CAPSGraph*)(implicit session: CAPSSession): CAPSUnionGraph = {
    CAPSUnionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))
  }
}

final case class CAPSUnionGraph(graphs: Map[CAPSGraph, Map[Int, Int]])
  (implicit val session: CAPSSession) extends CAPSGraph {

  require(graphs.nonEmpty, "Union requires at least one graph")

  override lazy val tags: Set[Int] = graphs.values.flatMap(_.values).toSet

  override def toString = s"CAPSUnionGraph(graphs=[${graphs.mkString(",")}])"

  override lazy val schema: CAPSSchema = {
    graphs.keys.map(g => g.schema).foldLeft(Schema.empty)(_ ++ _).asCaps
  }

  override def cache(): CAPSUnionGraph = map(_.cache())

  override def persist(): CAPSUnionGraph = map(_.persist())

  override def persist(storageLevel: StorageLevel): CAPSUnionGraph = map(_.persist(storageLevel))

  override def unpersist(): CAPSUnionGraph = map(_.unpersist())

  override def unpersist(blocking: Boolean): CAPSUnionGraph = map(_.unpersist(blocking))

  private def map(f: CAPSGraph => CAPSGraph): CAPSUnionGraph =
    CAPSUnionGraph(graphs.keys.map(f).zip(graphs.keys).toMap.mapValues(graphs))

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = schema.headerForNode(node)
    val nodeScans = graphs.keys
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map {
        graph =>
          val nodeScan = graph.nodes(name, nodeCypherType)
          nodeScan.retag(graphs(graph))
      }

    val alignedScans = nodeScans
      .map(_.alignWith(node, targetHeader))
      .map(_.select(targetHeader.expressions.toSeq.sorted: _*))

    alignedScans
      .reduceOption(_ unionAll _)
      .map(_.distinct(node))
      .getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = schema.headerForRelationship(rel)
    val relScans = graphs.keys
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        relScan.retag(graphs(graph))
      }

    val alignedScans = relScans
      .map(_.alignWith(rel, targetHeader))
      .map(_.select(targetHeader.expressions.toSeq.sorted: _*))

    alignedScans
      .reduceOption(_ unionAll _)
      .map(_.distinct(rel))
      .getOrElse(CAPSRecords.empty(targetHeader))
  }
}
