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

final case class CAPSUnionGraph(graphs: List[CAPSGraph], doUpdateTags: Boolean = true)(implicit val session: CAPSSession) extends CAPSGraph {

  val graphsWithUpdatedTags = if (doUpdateTags) {
    updateTags(graphs)
  } else {
    graphs
  }

  private def updateTags(graphsToUpdate: List[CAPSGraph]): List[CAPSGraph] = {
    val tagList = graphsToUpdate.foldLeft(List.empty[Int])(_ ++ _.tags)
    val tagSet = tagList.toSet
    val maxUsedTag = tagSet.max

    val conflictCounts: Map[Int, Int] = tagList
      .groupBy(tag => tag)
      .mapValues(_.size - 1)
      .filter { case (_, conflicts) => conflicts > 0 }

    val foldStart = GraphIdShiftIntermediateResult(List.empty, conflictCounts, maxUsedTag + 1)
    val GraphIdShiftIntermediateResult(shiftedGraphs, _, _) = graphsToUpdate.foldLeft(foldStart) {
      case (intermediate, nextGraph) =>
        intermediate.shift(nextGraph)
    }
    shiftedGraphs
  }

  private lazy val individualSchemas = graphsWithUpdatedTags.map(_.schema)

  override lazy val schema: CAPSSchema = individualSchemas.foldLeft(Schema.empty)(_ ++ _).asCaps

  override def cache(): CAPSUnionGraph = map(_.cache())

  override def persist(): CAPSUnionGraph = map(_.persist())

  override def persist(storageLevel: StorageLevel): CAPSUnionGraph = map(_.persist(storageLevel))

  override def unpersist(): CAPSUnionGraph = map(_.unpersist())

  override def unpersist(blocking: Boolean): CAPSUnionGraph = map(_.unpersist(blocking))

  private def map(f: CAPSGraph => CAPSGraph): CAPSUnionGraph = CAPSUnionGraph(graphsWithUpdatedTags.map(f), doUpdateTags)

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = RecordHeader.nodeFromSchema(node, schema)
    val nodeScans: Seq[CAPSRecords] = graphsWithUpdatedTags
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map(_.nodes(name, nodeCypherType))

    val alignedScans = nodeScans.map(_.alignWith(node, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll(targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, schema)
    val relScans: Seq[CAPSRecords] = graphsWithUpdatedTags
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map(_.relationships(name, relCypherType))
    val alignedScans = relScans.map(_.alignWith(rel, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll(targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def tags: Set[Int] = ???

  override def replaceTags(replacements: (Int, Int)*): CAPSGraph = ???
}

private[spark] case class GraphIdShiftIntermediateResult(shiftedGraphs: List[CAPSGraph], conflictCounts: Map[Int, Int], nextTag: Int) {
  def shift(nextGraph: CAPSGraph): GraphIdShiftIntermediateResult = {
    val conflicts = nextGraph.tags intersect conflictCounts.keySet
    if (conflicts.nonEmpty) {
      val nextTagAfterShifting = nextTag + conflicts.size + 1
      val replacements = conflicts.toList.zip(nextTag until nextTagAfterShifting)
      val shiftedGraph = nextGraph.replaceTags(replacements: _*)
      val updatedConflictCounts = conflictCounts.map {
        case (tag, count) if conflicts.contains(tag) => tag -> (count - 1)
        case (tag, count) => tag -> count
      }
      copy(shiftedGraphs = shiftedGraph :: shiftedGraphs, conflictCounts = updatedConflictCounts, nextTag = nextTagAfterShifting)
    } else {
      copy(shiftedGraphs = nextGraph :: shiftedGraphs)
    }
  }
}
