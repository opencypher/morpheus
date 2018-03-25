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
import org.opencypher.okapi.impl.schema.TagSupport
import org.opencypher.okapi.impl.schema.TagSupport._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

object CAPSUnionGraph {
  def apply(graphs: CAPSGraph*)(implicit session: CAPSSession): CAPSUnionGraph = {
    CAPSUnionGraph(graphs.toList)
  }
}

// TODO: flatten union trees
final case class CAPSUnionGraph(graphs: List[CAPSGraph], doUpdateTags: Boolean = true)(implicit val session: CAPSSession) extends CAPSGraph {
  require(graphs.size > 0, "Union requires at least one graph")

  private lazy val individualSchemas: Seq[CAPSSchema] = graphs.map(_.schema.asCaps)

  override lazy val schema: CAPSSchema = {
    if (doUpdateTags) {
      individualSchemas.reduce(_ union _)
    } else {
      individualSchemas.foldLeft(Schema.empty)(_ ++ _).asCaps
    }
  }

  override def cache(): CAPSUnionGraph = map(_.cache())

  override def persist(): CAPSUnionGraph = map(_.persist())

  override def persist(storageLevel: StorageLevel): CAPSUnionGraph = map(_.persist(storageLevel))

  override def unpersist(): CAPSUnionGraph = map(_.unpersist())

  override def unpersist(blocking: Boolean): CAPSUnionGraph = map(_.unpersist(blocking))

  private def map(f: CAPSGraph => CAPSGraph): CAPSUnionGraph = CAPSUnionGraph(graphs.map(f), doUpdateTags)

  def performedRetaggings: Map[CAPSGraph, Map[Int, Int]] = replacementMap

  private val replacementMap: Map[CAPSGraph, Map[Int, Int]] = {
    if (doUpdateTags) {
      val (result, _) = graphs.foldLeft((Map.empty[CAPSGraph, Map[Int, Int]], Set.empty[Int])) {

        case ((graphReplacements, previousTags), currentGraph) =>

          val rightTags = currentGraph.schema.tags

          val replacements = previousTags.replacementsFor(rightTags)
          val updatedRightTags = rightTags.replaceWith(replacements)

          val updatedPreviousTags = previousTags ++ updatedRightTags
          val updatedGraphReplacements = graphReplacements.updated(currentGraph, replacements)

          updatedGraphReplacements -> updatedPreviousTags
      }
      result
    } else {
      Map.empty.withDefaultValue(Map.empty)
    }
  }

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = RecordHeader.nodeFromSchema(node, schema)
    val nodeScans: Seq[CAPSRecords] = graphs
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map {
        graph =>
          val nodeScan = graph.nodes(name, nodeCypherType)
          nodeScan.replaceTags(replacementMap(graph))
      }

    val alignedScans = nodeScans.map(_.alignWith(node, targetHeader))

    alignedScans
      .reduceOption(_ unionAll(targetHeader, _))
      .map(_.distinct(node))
      .getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, schema)
    val relScans: Seq[CAPSRecords] = graphs
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        relScan.replaceTags(replacementMap(graph))
      }

    val alignedScans = relScans.map(_.alignWith(rel, targetHeader))
    alignedScans
      .reduceOption(_ unionAll(targetHeader, _))
      .map(_.distinct(rel))
      .getOrElse(CAPSRecords.empty(targetHeader))
  }
}
