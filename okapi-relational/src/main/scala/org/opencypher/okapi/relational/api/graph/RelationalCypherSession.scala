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
package org.opencypher.okapi.relational.api.graph

import java.util.concurrent.atomic.AtomicLong

import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph.{CypherSession, GraphName, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.graph.QGNGenerator
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintOptimizedRelationalPlan, PrintQueryExecutionStages, PrintRelationalPlan}
import org.opencypher.okapi.relational.api.planning.{RelationalCypherResult, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory, Table}
import org.opencypher.okapi.relational.impl.RelationalConverters._
import org.opencypher.okapi.relational.impl.planning.{RelationalOptimizer, RelationalPlanner}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

abstract class RelationalCypherSession[T <: Table[T] : TypeTag] extends CypherSession {

  type Graph <: RelationalCypherGraph[T]

  type Records <: RelationalCypherRecords[T]

  override type Result = RelationalCypherResult[T]

  implicit val session: RelationalCypherSession[T] = this

  protected val logicalPlanner: LogicalPlanner = new LogicalPlanner(new LogicalOperatorProducer)

  protected val logicalOptimizer: LogicalOptimizer.type = LogicalOptimizer

  protected val parser: CypherParser = CypherParser

  private[opencypher] val tableTypeTag: TypeTag[T] = implicitly[TypeTag[T]]

  private[opencypher] def records: RelationalCypherRecordsFactory[T]

  private[opencypher] def graphs: RelationalCypherGraphFactory[T]

  private[opencypher] def graphAt(qgn: QualifiedGraphName): Option[RelationalCypherGraph[T]] =
    if (catalog.graphNames.contains(qgn)) Some(catalog.graph(qgn).asRelational) else None

  private[opencypher] def basicRuntimeContext(parameters: CypherMap = CypherMap.empty): RelationalRuntimeContext[T] =
    RelationalRuntimeContext(graphAt, parameters = parameters)(this)

  private[opencypher] def time[R](description: String)(code: => R): R = {
    if (PrintTimings.isSet) printTiming(description)(code) else code
  }

  private[opencypher] def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val qgn = qgnGenerator.generate
    catalog.store(qgn, ambient)
    IRCatalogGraph(qgn, ambient.schema)
  }

  private val maxSessionGraphId: AtomicLong = new AtomicLong(0)

  private[opencypher] val qgnGenerator = new QGNGenerator {
    override def generate: QualifiedGraphName = {
      QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName(s"tmp${maxSessionGraphId.incrementAndGet}"))
    }
  }

  override def generateQualifiedGraphName: QualifiedGraphName = qgnGenerator.generate

  override def cypher(
    query: String,
    parameters: CypherMap = CypherMap.empty,
    drivingTable: Option[CypherRecords] = None,
    queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty
  ): Result = cypherOnGraph(graphs.empty, query, parameters, drivingTable, queryCatalog)

  protected def planCypherQuery(
    graph: PropertyGraph,
    cypherQuery: CypherQuery,
    allParameters: CypherMap,
    inputFields: Set[Var],
    maybeDrivingTable: Option[RelationalCypherRecords[T]],
    queryLocalCatalog: QueryLocalCatalog
  ): Result = {
    val logicalPlan = planLogical(cypherQuery, graph, inputFields)
    planRelational(maybeDrivingTable, allParameters, logicalPlan, queryLocalCatalog)
  }

  protected def planLogical(ir: CypherQuery, graph: PropertyGraph, inputFields: Set[Var]): LogicalOperator = {
    logStageProgress("Logical planning ...", newLine = false)
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty)
    }

    logStageProgress("Logical optimization ...", newLine = false)
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Optimized logical plan:")
      optimizedLogicalPlan.show()
    }
    optimizedLogicalPlan
  }

  protected def planRelational(
    maybeDrivingTable: Option[RelationalCypherRecords[T]],
    parameters: CypherMap,
    logicalPlan: LogicalOperator,
    queryLocalCatalog: QueryLocalCatalog
  ): Result = {
    logStageProgress("Relational planning ... ", newLine = false)
    def queryLocalGraphAt(qgn: QualifiedGraphName): Option[RelationalCypherGraph[T]] = {
      Try(new RichPropertyGraph(queryLocalCatalog.graph(qgn)).asRelational[T]).toOption
    }
    implicit val context: RelationalRuntimeContext[T] = RelationalRuntimeContext(queryLocalGraphAt, maybeDrivingTable, parameters)

    val relationalPlan = time("Relational planning")(RelationalPlanner.process(logicalPlan))
    logStageProgress("Done!")
    if (PrintRelationalPlan.isSet) {
      println("Relational plan:")
      relationalPlan.show()
    }

    logStageProgress("Relational optimization ... ", newLine = false)
    val optimizedRelationalPlan = time("Relational optimization")(RelationalOptimizer.process(relationalPlan))
    logStageProgress("Done!")
    if (PrintOptimizedRelationalPlan.isSet) {
      println("Optimized Relational plan:")
      optimizedRelationalPlan.show()
    }

    RelationalCypherResult(logicalPlan, optimizedRelationalPlan)
  }

  protected def logStageProgress(s: String, newLine: Boolean = true): Unit = {
    if (PrintQueryExecutionStages.isSet) {
      if (newLine) {
        println(s)
      } else {
        val padded = s.padTo(30, " ").mkString("")
        print(padded)
      }
    }
  }
}
