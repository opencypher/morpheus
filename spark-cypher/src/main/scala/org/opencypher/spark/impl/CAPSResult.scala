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
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.physical.CAPSQueryPlans

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

trait CAPSResult extends CypherResult {

  type Graph = CAPSGraph

  override def records: Option[CAPSRecords]

  override def getRecords: CAPSRecords = records.get

  override def graph: Option[Graph]

  override def getGraph: Graph = graph.get

  def as[E <: Product : TypeTag]: Iterator[E] = {
    records match {
      case Some(r) =>
        implicit val encoder = ExpressionEncoder[E]
        r.asCaps.data.as[E].toLocalIterator().asScala
      case None =>
        Iterator.empty
    }
  }

  override def show(implicit options: PrintOptions): Unit =
    records match {
      case Some(r) => r.show
      case None => options.stream.print("No results")
    }

  override def plans: CAPSQueryPlans

  override def toString = this.getClass.getSimpleName
}

object CAPSResult {
  def empty(queryPlans: CAPSQueryPlans = CAPSQueryPlans.empty): CAPSResult = new CAPSResult {
    override def records: Option[CAPSRecords] = None

    override def graph = None

    override def plans: CAPSQueryPlans = queryPlans
  }
}