/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.impl.graph

import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, Start}
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.reflect.runtime.universe.TypeTag

sealed case class EmptyGraph[T <: Table[T] : TypeTag]()(implicit val session: RelationalCypherSession[T]) extends RelationalCypherGraph[T] {

  override type Session = RelationalCypherSession[T]

  override type Records = RelationalCypherRecords[T]

  override val schema: Schema = Schema.empty

  override def cache(): EmptyGraph[T] = this

  override def tables: Seq[T] = Seq.empty

  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean): RelationalOperator[T] = {
    implicit val context: RelationalRuntimeContext[T] = session.basicRuntimeContext()

    val scanHeader = searchPattern.entities
      .map { e => RecordHeader.from(e.toVar)}
      .reduce(_ ++ _)

    val records = session.records.empty(scanHeader)
    Start.fromEmptyGraph(records)
  }
}
