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
package org.opencypher.caps.api.graph

import org.opencypher.caps.impl.record.{CypherPrintable, CypherRecords}
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.trees.TreeNode

/**
  * Describes the result of executing a Cypher query.
  *
  * The result of a Cypher query consists of a table of records and a set of named graphs.
  */
trait CypherResult extends CypherPrintable {

  /**
    * Retrieves the single graph returned by the query, if it returned exactly one graph.
    *
    * @return the single graph, otherwise None.
    */
  def singleGraph: Option[PropertyGraph] = if (graphs.size == 1) Some(graphs.head._2) else None

  /**
    * The named graphs that were returned by the query that produced this result.
    *
    * @return a map of named graphs.
    */
  def graphs: Map[String, PropertyGraph]

  /**
    * The table of records that was returned by the query that produced this result.
    *
    * @return a table of records.
    */
  def records: CypherRecords

  type LogicalPlan <: TreeNode[LogicalPlan]
  type FlatPlan <: TreeNode[FlatPlan]
  type PhysicalPlan <: TreeNode[PhysicalPlan]

  def explain: Plan[LogicalPlan, FlatPlan, PhysicalPlan]

}

case class Plan[L <: TreeNode[L], F <: TreeNode[F], P <: TreeNode[P]](
    logical: CypherResultPlan[L],
    private[caps] val flat: CypherResultPlan[F],
    private[caps] val physical: CypherResultPlan[P])
    extends CypherPrintable {
  override def print(implicit options: PrintOptions): Unit = logical.print
}
