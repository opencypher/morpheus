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
import org.opencypher.caps.api.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.{CypherResult, PlaceholderCypherValue, PropertyGraph}
import org.opencypher.caps.api.io.{CreateOrFail, PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.value.CAPSValue
import org.opencypher.caps.demo.Configuration.{PrintLogicalPlan, PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.record.CypherRecords
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.io.{CAPSGraphSourceHandler, CAPSPropertyGraphDataSource}
import org.opencypher.caps.impl.spark.physical._
import org.opencypher.caps.impl.util.parsePathOrURI
import org.opencypher.caps.ir.api.expr.{Expr, Var}
import org.opencypher.caps.ir.api.{IRExternalGraph, IRField}
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.impl._

sealed class CAPSSessionImpl(val sparkSession: SparkSession, private val graphSourceHandler: CAPSGraphSourceHandler)
    extends CAPSSession
    with Serializable
    with CAPSSessionOps {

  self =>

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner()
  private val physicalOptimizer = new PhysicalOptimizer()
  private val parser = CypherParser

  def sourceAt(uri: URI): PropertyGraphDataSource =
    graphSourceHandler.sourceAt(uri)(this)

  override def readFrom(path: String): PropertyGraph =
    readFrom(parsePathOrURI(path))

  // TODO: why not Option[CAPSGraph] in general?
  override def readFrom(uri: URI): PropertyGraph =
    graphSourceHandler.sourceAt(uri)(this).graph

  def optGraphAt(uri: URI): Option[PropertyGraph] =
    graphSourceHandler.optSourceAt(uri)(this).map(_.graph)

  override def write(graph: PropertyGraph, pathOrUri: String, mode: PersistMode = CreateOrFail): Unit =
    graphSourceHandler.sourceAt(parsePathOrURI(pathOrUri))(this).store(graph, mode)

  override def mount(source: PropertyGraphDataSource, path: String): Unit = source match {
    case c: CAPSPropertyGraphDataSource => mountSourceAt(c, parsePathOrURI(path))
    case x                              => throw UnsupportedOperationException(s"can only handle CAPS graph sources, but got $x")
  }

  def mountSourceAt(source: CAPSPropertyGraphDataSource, uri: URI): Unit =
    graphSourceHandler.mountSourceAt(source, uri)(this)

  def unmountAll(): Unit =
    graphSourceHandler.unmountAll(this)

  override def cypher(query: String, parameters: Map[String, PlaceholderCypherValue]): CypherResult =
    cypherOnGraph(CAPSGraph.empty(this), query, parameters)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: Map[String, PlaceholderCypherValue]): CypherResult = {
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = parser.process(query)(CypherParser.defaultContext)

    val extractedParameters = extractedLiterals.mapValues(v => CAPSValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR ...", false)
    val ir = IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, sourceAt))
    logStageProgress("Done!")

    logStageProgress("Logical plan ...", false)
    val logicalPlannerContext =
      LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.andThen(sourceAt), ambientGraph)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logStageProgress("Done!")

    logStageProgress("Optimizing logical plan ...", false)
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    logStageProgress("Done!")

    if (PrintLogicalPlan.get()) {
      println("Logical plan:")
      println(logicalPlan.pretty())
      println("Optimized logical plan:")
      println(optimizedLogicalPlan.pretty())
    }

    plan(graph, CAPSRecords.unit()(this), allParameters, optimizedLogicalPlan)
  }

  private def logStageProgress(s: String, newLine: Boolean = true): Unit = {
    if (PrintQueryExecutionStages.get()) {
      if (newLine) {
        println(s)
      } else {
        val padded = s.padTo(30, " ").mkString("")
        print(padded)
      }
    }
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val name = UUID.randomUUID().toString
    val uri = URI.create(s"session:///graphs/ambient/$name")

    val graphSource = new CAPSPropertyGraphDataSource {
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
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalExternalGraph(ambientGraph.name, ambientGraph.uri, graph.schema), fields)
  }

  override def filter(
      graph: PropertyGraph,
      in: CypherRecords,
      expr: Expr,
      queryParameters: Map[String, PlaceholderCypherValue]): CAPSRecords = {
    val scan = planStart(graph, in.header.asCaps.internalHeader.fields)
    val filter = producer.planFilter(expr, scan)
    plan(graph, in, queryParameters, filter).records
  }

  override def select(
      graph: PropertyGraph,
      in: CypherRecords,
      fields: IndexedSeq[Var],
      queryParameters: Map[String, PlaceholderCypherValue]): CAPSRecords = {
    val scan = planStart(graph, in.header.asCaps.internalHeader.fields)
    val select = producer.planSelect(fields, Set.empty, scan)
    plan(graph, in, queryParameters, select).records
  }

  override def project(
      graph: PropertyGraph,
      in: CypherRecords,
      expr: Expr,
      queryParameters: Map[String, PlaceholderCypherValue]): CAPSRecords = {
    val scan = planStart(graph, in.header.asCaps.internalHeader.fields)
    val project = producer.projectExpr(expr, scan)
    plan(graph, in, queryParameters, project).records
  }

  override def alias(
      graph: PropertyGraph,
      in: CypherRecords,
      alias: (Expr, Var),
      queryParameters: Map[String, PlaceholderCypherValue]): CAPSRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.header.asCaps.internalHeader.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(
      graph: PropertyGraph,
      records: CypherRecords,
      parameters: Map[String, PlaceholderCypherValue],
      logicalPlan: LogicalOperator): CAPSResult = {
    logStageProgress("Flat plan ... ", false)
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(parameters))
    logStageProgress("Done!")

    logStageProgress("Physical plan ... ", false)
    val physicalPlannerContext = PhysicalPlannerContext.from(readFrom, records.asCaps, parameters)
    val physicalPlan = physicalPlanner(flatPlan)(physicalPlannerContext)
    logStageProgress("Done!")

    if (PrintPhysicalPlan.get()) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }

    logStageProgress("Optimizing physical plan ... ", false)
    val optimizedPhysicalPlan = physicalOptimizer(physicalPlan)(PhysicalOptimizerContext())
    logStageProgress("Done!")

    if (PrintPhysicalPlan.get()) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty())
    }

    CAPSResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      RuntimeContext(physicalPlannerContext.parameters, optGraphAt, collection.mutable.Map.empty))
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
