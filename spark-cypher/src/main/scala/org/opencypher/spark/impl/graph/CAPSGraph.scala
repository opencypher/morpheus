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
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

object CAPSGraph {

  type CAPSGraph = RelationalCypherGraph[DataFrameTable]

  def empty(implicit caps: CAPSSession): CAPSGraph = EmptyGraph()

  def create(nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*)(implicit caps: CAPSSession): CAPSGraph = {
    create(Set(0), None, nodeTable, entityTables: _*)
  }

  def create(maybeSchema: Option[CAPSSchema], nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*)(implicit caps: CAPSSession): CAPSGraph = {
    create(Set(0), maybeSchema, nodeTable, entityTables: _*)
  }

  def create(tags: Set[Int], maybeSchema: Option[CAPSSchema], nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*)(implicit caps: CAPSSession): CAPSGraph = {
    val allTables = nodeTable +: entityTables
    val schema = maybeSchema.getOrElse(allTables.map(_.schema).reduce[Schema](_ ++ _).asCaps)
    new CAPSScanGraph(allTables, schema, tags)
  }

  def create(records: CypherRecords, schema: CAPSSchema, tags: Set[Int] = Set(0))(implicit caps: CAPSSession): CAPSGraph = {
    val capsRecords = records.asCaps
    SingleTableGraph(capsRecords, schema, tags)
  }

  sealed case class EmptyGraph(implicit val caps: CAPSSession) extends RelationalCypherGraph[DataFrameTable] {

    override type Session = CAPSSession

    override type Records = CAPSRecords

    override val schema: CAPSSchema = CAPSSchema.empty

    override def nodes(name: String, cypherType: CTNode, exactLabelMatch: Boolean = false): CAPSRecords =
      caps.records.empty(RecordHeader.from(Var(name)(cypherType)))

    override def relationships(name: String, cypherType: CTRelationship): CAPSRecords =
      caps.records.empty(RecordHeader.from(Var(name)(cypherType)))

    override def session: CAPSSession = caps

    override def cache(): CAPSGraph = this


    override def tags: Set[Int] = Set.empty
  }

}
