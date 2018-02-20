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
package org.opencypher.caps.impl.spark

import java.net.URI
import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.configuration.CoraConfiguration.{PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.caps.api.graph.{CypherResult, PropertyGraph}
import org.opencypher.caps.api.io._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.exception.UnsupportedOperationException
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.physical.PhysicalPlanner
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.io.session.SessionPropertyGraphDataSourceOld
import org.opencypher.caps.impl.spark.io.{CAPSGraphSourceHandler, CAPSPropertyGraphDataSourceOld}
import org.opencypher.caps.impl.spark.physical._
import org.opencypher.caps.impl.util.parsePathOrURI
import org.opencypher.caps.ir.api.expr.{Expr, Var}
import org.opencypher.caps.ir.api.{IRExternalGraph, IRExternalGraphNew, IRField}
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.caps.logical.impl._

sealed class CAPSSessionImpl(val sparkSession: SparkSession, private val graphSourceHandler: CAPSGraphSourceHandler)
  extends CAPSSession
    with Serializable
    with CAPSSessionOps {

  self =>

  private implicit def capsSession = this
  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner(new CAPSPhysicalOperatorProducer()(self))
  private val physicalOptimizer = new PhysicalOptimizer()
  private val parser = CypherParser

  def sourceAt(uri: URI): PropertyGraphDataSourceOld =
    graphSourceHandler.sourceAt(uri)

  override def readFrom(path: String): PropertyGraph =
    readFrom(parsePathOrURI(path))

  // TODO: why not Option[CAPSGraph] in general?
  override def readFrom(uri: URI): PropertyGraph =
    graphSourceHandler.sourceAt(uri).graph

  def optGraphAt(uri: URI): Option[CAPSGraph] =
    graphSourceHandler.optSourceAt(uri).map(_.graph) match {
      case Some(graph) => Some(graph.asCaps)
      case None => None
    }

  def optGraphAtNew(qualifiedGraphName: QualifiedGraphName): Option[CAPSGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asCaps)

  override def write(graph: PropertyGraph, pathOrUri: String, mode: PersistMode = CreateOrFail): Unit =
    graphSourceHandler.sourceAt(parsePathOrURI(pathOrUri)).store(graph, mode)

  override def mount(path: String, graph: PropertyGraph): Unit = {
    val source = SessionPropertyGraphDataSourceOld(path)
    source.store(graph, CreateOrFail)
    mount(source, path)
  }

  override def mount(source: PropertyGraphDataSourceOld, path: String): Unit = source match {
    case c: CAPSPropertyGraphDataSourceOld => mountSourceAt(c, parsePathOrURI(path))
    case x => throw UnsupportedOperationException(s"can only handle CAPS graph sources, but got $x")
  }

  def mountSourceAt(source: CAPSPropertyGraphDataSourceOld, uri: URI): Unit =
    graphSourceHandler.mountSourceAt(source, uri)

  def unmountAll(): Unit =
    graphSourceHandler.unmountAll

  override def sql(query: String): CAPSRecords =
    CAPSRecords.wrap(sparkSession.sql(query))

  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPSGraph.empty, query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraph = mountAmbientGraph(graph)
    val ambientGraphNew = mountAmbientGraphNew(graph)


    val (stmt, extractedLiterals, semState) = parser.process(query)(CypherParser.defaultContext)

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR ...", newLine = false)
    val ir = IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, ambientGraphNew, sourceAt, dataSource))
    logStageProgress("Done!")

    logStageProgress("Logical plan ...", newLine = false)
    val logicalPlannerContext =
      LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraphNew)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logStageProgress("Done!")

    logStageProgress("Optimizing logical plan ...", newLine = false)
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    logStageProgress("Done!")

    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty())
      println("Optimized logical plan:")
      println(optimizedLogicalPlan.pretty())
    }

    plan(graph, CAPSRecords.unit(), allParameters, optimizedLogicalPlan)
  }

  private def logStageProgress(s: String, newLine: Boolean = true): Unit = {
    if (PrintQueryExecutionStages.isSet) {
      if (newLine) {
        println(s)
      } else {
        val padded = s.padTo(30, " ").mkString("")
        print(padded)
      }
    }
  }

  private def mountAmbientGraphNew(ambient: PropertyGraph): IRExternalGraphNew = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = mount(graphName, ambient)
    IRExternalGraphNew(graphName.value, ambient.schema, qualifiedGraphName)
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val name = UUID.randomUUID().toString
    val uri = URI.create(s"session:///graphs/ambient/$name")

    val graphSource = new CAPSPropertyGraphDataSourceOld {
      override def schema: Option[Schema] = Some(graph.schema)

      override def canonicalURI: URI = uri

      override def delete(): Unit = throw UnsupportedOperationException("Deletion of an ambient graph")

      override def graph: PropertyGraph = ambient

      override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI

      override def create: CAPSGraph = throw UnsupportedOperationException("Creation of an ambient graph")

      override def store(graph: PropertyGraph, mode: PersistMode): CAPSGraph =
        throw UnsupportedOperationException("Persisting an ambient graph")

      override val session: CAPSSession = self
    }

    graphSourceHandler.mountSourceAt(graphSource, uri)(self)

    IRExternalGraph(name, ambient.schema, uri)
  }

  private def planStart(graph: PropertyGraph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraphNew(graph)

    producer.planStart(LogicalExternalGraph(ambientGraph.name, ambientGraph.qualifiedName, graph.schema), fields)
  }

  override def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val filter = producer.planFilter(expr, scan)
    plan(graph, in, queryParameters, filter).records
  }

  override def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: IndexedSeq[Var],
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.planSelect(fields, Set.empty, scan)
    plan(graph, in, queryParameters, select).records
  }

  override def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val project = producer.projectExpr(expr, scan)
    plan(graph, in, queryParameters, project).records
  }

  override def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CAPSRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(
    graph: PropertyGraph,
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator): CAPSResult = {
    logStageProgress("Flat plan ... ", newLine = false)
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(parameters))
    logStageProgress("Done!")

    logStageProgress("Physical plan ... ", newLine = false)
    val physicalPlannerContext = CAPSPhysicalPlannerContext.from(readFrom, records.asCaps, parameters)(self)
    val physicalPlan = physicalPlanner(flatPlan)(physicalPlannerContext)
    logStageProgress("Done!")

    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }

    logStageProgress("Optimizing physical plan ... ", newLine = false)
    val optimizedPhysicalPlan = physicalOptimizer(physicalPlan)(PhysicalOptimizerContext())
    logStageProgress("Done!")

    if (PrintPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty())
    }

    CAPSResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      CAPSRuntimeContext(physicalPlannerContext.parameters, optGraphAtNew, collection.mutable.Map.empty))
  }

  override def toString: String = {
    val mountPoints = graphSourceHandler.sessionGraphSourceFactory.mountPoints.keys

    val mountPointsString =
      if (mountPoints.nonEmpty)
        s"mountPoints: ${mountPoints.mkString(", ")}"
      else "No graphs mounted"

    s"${this.getClass.getSimpleName}($mountPointsString)"
  }

}
