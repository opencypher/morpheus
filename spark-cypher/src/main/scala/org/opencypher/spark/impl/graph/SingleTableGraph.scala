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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.operators.{Distinct, ExtractEntities, RelationalOperator, Start}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

/**
  * A single table graph represents the result of CONSTRUCT clause. It contains all entities from the outer scope that 
  * the clause constructs. The initial schema of that graph is the union of all graph schemata the CONSTRUCT clause refers
  * to, including their corresponding graph tags. Note, that the initial schema does not include the graph tag used for
  * the constructed entities.
  */
case class SingleTableGraph(
  baseTable: RelationalCypherRecords[DataFrameTable],
  override val schema: Schema,
  override val tags: Set[Int]
)(implicit val session: CAPSSession, context: RelationalRuntimeContext[DataFrameTable])
  extends RelationalCypherGraph[DataFrameTable] {

  override type Session = CAPSSession

  override type Records = CAPSRecords

  private val header = baseTable.header

  def show(): Unit = baseTable.show

  override def tables: Seq[DataFrameTable] = Seq(baseTable.table)

  override private[opencypher] def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[DataFrameTable] = {
    val entity = Var("")(entityType)
    val targetEntityHeader = schema.headerForEntity(entity)
    val extractionVars: Set[Var] = header.entitiesForType(entityType)
    Distinct(ExtractEntities(Start(baseTable), targetEntityHeader, extractionVars), Set(entity))
  }

}
