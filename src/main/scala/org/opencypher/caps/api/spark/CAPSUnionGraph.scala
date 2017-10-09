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

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{GraphScan, RecordHeader}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}

case class CAPSUnionGraph(graphs: CAPSGraph*)
                         (implicit val session: CAPSSession) extends CAPSGraph {

  override protected def graph: CAPSGraph = this

  private lazy val individualSchemas = graphs.map(_.schema)
  private lazy val individualTokens = graphs.map(_.tokens)

  override lazy val schema = individualSchemas.reduceOption(_ ++ _).getOrElse(Schema.empty)
  override lazy val tokens = individualTokens.reduceOption(_ ++ _).getOrElse(CAPSTokens.empty)

  override def nodes(name: String, nodeCypherType: CTNode) = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = RecordHeader.nodeFromSchema(node, schema, tokens.registry)
    val nodeScans: Seq[CAPSRecords] = graphs
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map(_.nodes(name, nodeCypherType))
    val alignedScans = nodeScans.map(GraphScan.align(_, node, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll(targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship) = {
    val rel = Var(name)(relCypherType)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, schema, tokens.registry)
    val relScans: Seq[CAPSRecords] = graphs
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map(_.relationships(name, relCypherType))
    val alignedScans = relScans.map(GraphScan.align(_, rel, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll(targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  // TODO: Flatten
  override def union(other: CAPSGraph) = CAPSUnionGraph(this, other)

}
