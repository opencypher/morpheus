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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.okapi.api.graph.CypherQueryPlans
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.{QueryPlans, RelationalCypherResult}
import org.opencypher.okapi.relational.impl.operators.RelationalOperator
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

case class CAPSResult(logical: Option[LogicalOperator], relational: Option[RelationalOperator[DataFrameTable]]) extends RelationalCypherResult[DataFrameTable] {

  override type Graph = CAPSGraph

  override def getRecords: Option[CAPSRecords] = relational.map(op => CAPSRecords(op.header, op.table, op.returnItems.map(_.map(_.withoutType))))

  override def records: CAPSRecords = getRecords.get

  override def getGraph: Option[CAPSGraph] = relational.map(_.graph.asCaps)

  override def graph: CAPSGraph = getGraph.get.asCaps

  def as[E <: Product : TypeTag]: Iterator[E] = {
    getRecords match {
      case Some(r) =>
        implicit val encoder = ExpressionEncoder[E]
        r.asCaps.df.as[E].toLocalIterator().asScala
      case None =>
        Iterator.empty
    }
  }

  override def show(implicit options: PrintOptions): Unit =
    getRecords match {
      case Some(r) => r.show
      case None => options.stream.print("No results")
    }

  override def plans: CypherQueryPlans = QueryPlans(logical, relational)

  override def toString: String = this.getClass.getSimpleName
}

object CAPSResult {

  val empty: CAPSResult =
    CAPSResult(None, None)

  def apply(logical: LogicalOperator, relational: RelationalOperator[DataFrameTable]): CAPSResult =
    CAPSResult(Some(logical), Some(relational))
}
