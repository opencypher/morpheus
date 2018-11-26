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

import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph.{CypherSession, GraphName, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintOptimizedRelationalPlan, PrintQueryExecutionStages, PrintRelationalPlan}
import org.opencypher.okapi.relational.api.io.{EntityTable, NodeTable}
import org.opencypher.okapi.relational.api.planning.{RelationalCypherResult, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory, Table}
import org.opencypher.okapi.relational.impl.RelationalConverters._
import org.opencypher.okapi.relational.impl.planning.{RelationalOptimizer, RelationalPlanner}

import scala.reflect.runtime.universe.TypeTag

/**
  * Base class for relational back ends implementing the OKAPI pipeline.
  *
  * The class provides a generic implementation of the necessary steps to execute a Cypher query on a relational back
  * end including parsing, IR planning, logical planning and relational planning.
  *
  * Implementers need to make sure to provide factories for back end specific record and graph implementations.
  *
  * @tparam T back end specific [[Table]] implementation
  */
abstract class RelationalCypherSession[T <: Table[T] : TypeTag] extends CypherSession {

  /**
    * Back end specific graph type
    */
  type Graph <: RelationalCypherGraph[T]

  /**
    * Back end specific records type
    */
  type Records <: RelationalCypherRecords[T]

  override type Result = RelationalCypherResult[T]

  private implicit val session: RelationalCypherSession[T] = this

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(nodeTable: NodeTable[T], entityTables: EntityTable[T]*): PropertyGraph = {
    graphs.create(nodeTable, entityTables: _ *)
  }

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param tags         tags that are used by graph entities
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(tags: Set[Int], nodeTable: NodeTable[T], entityTables: EntityTable[T]*): PropertyGraph = {
    graphs.create(tags, None, nodeTable, entityTables: _*)
  }

  /**
    * Qualified graph name for the empty graph
    */
  private[opencypher] lazy val emptyGraphQgn = QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName("emptyGraph"))

  //   Store empty graph in catalog, so operators that start with an empty graph can refer to its QGN
  override lazy val catalog: CypherCatalog = CypherCatalog(emptyGraphQgn -> graphs.empty)

  protected val parser: CypherParser = CypherParser

  protected val logicalPlanner: LogicalPlanner = new LogicalPlanner(new LogicalOperatorProducer)

  protected val logicalOptimizer: LogicalOptimizer.type = LogicalOptimizer

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

  override def cypher(
    query: String,
    parameters: CypherMap = CypherMap.empty,
    drivingTable: Option[CypherRecords] = None,
    queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty
  ): Result = cypherOnGraph(graphs.empty, query, parameters, drivingTable, queryCatalog)

  override private[opencypher] def cypherOnGraph(
    graph: PropertyGraph,
    query: String,
    queryParameters: CypherMap,
    maybeDrivingTable: Option[CypherRecords],
    queryCatalog: Map[QualifiedGraphName, PropertyGraph]
  ): Result = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val maybeRelationalRecords: Option[RelationalCypherRecords[T]] = maybeDrivingTable.map(_.asRelational)

    val inputFields = maybeRelationalRecords match {
      case Some(inputRecords) => inputRecords.header.vars
      case None => Set.empty[Var]
    }

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query, inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)

    val irBuilderContext = IRBuilderContext.initial(
      query,
      allParameters,
      semState,
      ambientGraphNew,
      qgnGenerator,
      catalog.listSources,
      catalog.view,
      inputFields,
      queryCatalog
    )
    val irOut = time("IR translation")(IRBuilder.process(stmt)(irBuilderContext))

    val ir = IRBuilder.extract(irOut)
    val queryLocalCatalog = IRBuilder.getContext(irOut).queryLocalCatalog

    logStageProgress("Done!")

    def processIR(ir: CypherStatement): RelationalCypherResult[T] = ir match {
      case cq: CypherQuery =>
        if (PrintIr.isSet) {
          println("IR:")
          println(cq.pretty)
        }
        planCypherQuery(graph, cq, allParameters, inputFields, maybeRelationalRecords, queryLocalCatalog)

      case CreateGraphStatement(targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, maybeRelationalRecords, queryLocalCatalog)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph)
        RelationalCypherResult.empty

      case CreateViewStatement(qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        RelationalCypherResult.empty

      case DeleteGraphStatement(qgn) =>
        catalog.dropGraph(qgn)
        RelationalCypherResult.empty

      case DeleteViewStatement(qgn) =>
        catalog.dropView(qgn)
        RelationalCypherResult.empty
    }

    processIR(ir)
  }

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

    def queryLocalGraphAt(qgn: QualifiedGraphName): Option[RelationalCypherGraph[T]] =
      Some(new RichPropertyGraph(queryLocalCatalog.graph(qgn)).asRelational[T])

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
