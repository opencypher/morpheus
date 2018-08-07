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
package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.graph.{SingleTableGraph, UnionGraph}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

case class CAPSGraphFactory(implicit caps: CAPSSession) extends RelationalCypherGraphFactory[DataFrameTable] {

  override type Graph = RelationalCypherGraph[DataFrameTable]

  override val empty: Graph = EmptyGraph()

  override def singleTableGraph(
    records: RelationalCypherRecords[DataFrameTable],
    schema: Schema,
    tagsUsed: Set[Int]
  )(implicit context: RelationalRuntimeContext[DataFrameTable]): Graph = SingleTableGraph(records, schema, tagsUsed)

  override def unionGraph(graphsToReplacements: Map[RelationalCypherGraph[DataFrameTable], Map[Int, Int]])
    (implicit context: RelationalRuntimeContext[DataFrameTable]): Graph = {
    UnionGraph(graphsToReplacements)
  }

  def create(nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*): Graph = {
    create(Set(0), None, nodeTable, entityTables: _*)
  }

  def create(maybeSchema: Option[CAPSSchema], nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*): Graph = {
    create(Set(0), maybeSchema, nodeTable, entityTables: _*)
  }

  def create(
    tags: Set[Int],
    maybeSchema: Option[CAPSSchema],
    nodeTable: CAPSNodeTable,
    entityTables: CAPSEntityTable*
  ): Graph = {
    implicit val runtimeContext: RelationalRuntimeContext[DataFrameTable] = caps.basicRuntimeContext()
    val allTables = nodeTable +: entityTables
    val schema = maybeSchema.getOrElse(allTables.map(_.schema).reduce[Schema](_ ++ _).asCaps)
    new CAPSScanGraph(allTables, schema, tags)
  }

  // TODO: only used in tests, move there
  def create(records: CypherRecords, schema: CAPSSchema, tags: Set[Int] = Set(0)): Graph = {
    implicit val runtimeContext: RelationalRuntimeContext[DataFrameTable] = caps.basicRuntimeContext()
    val capsRecords = records.asCaps
    SingleTableGraph(capsRecords, schema, tags)
  }
}
