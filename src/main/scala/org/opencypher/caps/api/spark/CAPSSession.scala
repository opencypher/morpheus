/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.spark

import java.net.URI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.caps.api.expr.{Expr, Var}
import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.api.io.{CreateOrFail, PersistMode}
import org.opencypher.caps.api.spark.io.{CAPSGraphSource, CAPSGraphSourceFactory}
import org.opencypher.caps.api.util.parsePathOrURI
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.logical._
import org.opencypher.caps.impl.parse.CypherParser
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceHandler
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvGraphSourceFactory
import org.opencypher.caps.impl.spark.io.neo4j.Neo4jGraphSourceFactory
import org.opencypher.caps.impl.spark.io.session.SessionGraphSourceFactory
import org.opencypher.caps.impl.spark.physical.{CAPSResultBuilder, PhysicalPlanner, PhysicalPlannerContext}
import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.ir.api.global.{ConstantRef, ConstantRegistry, GlobalsRegistry, TokenRegistry}
import org.opencypher.caps.ir.impl.global.GlobalsExtractor
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}

sealed class CAPSSession private(val sparkSession: SparkSession,
                                 private val graphSourceHandler: CAPSGraphSourceHandler)
  extends CypherSession with Serializable {

  override type Graph = CAPSGraph
  override type Session = CAPSSession
  override type Records = CAPSRecords
  override type Result = CAPSResult
  override type Data = DataFrame

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = new LogicalOptimizer(producer)
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner()
  private val parser = CypherParser

  def sourceAt(uri: URI): CAPSGraphSource =
    graphSourceHandler.sourceAt(uri)(this)

  def graphAt(path: String): CAPSGraph =
    graphAt(parsePathOrURI(path))

  def graphAt(uri: URI): CAPSGraph =
    graphSourceHandler.sourceAt(uri)(this).graph

  def persistGraphAt(graph: CAPSGraph, pathOrUri: String, mode: PersistMode = CreateOrFail): CAPSGraph =
    graphSourceHandler.sourceAt(parsePathOrURI(pathOrUri))(this).persist(graph, mode)

  def mountSourceAt(source: CAPSGraphSource, pathOrUri: String): Unit =
    mountSourceAt(source, parsePathOrURI(pathOrUri))

  def mountSourceAt(source: CAPSGraphSource, uri: URI): Unit =
    graphSourceHandler.mountSourceAt(source, uri)(this)

  def unmountAll(): Unit =
    graphSourceHandler.unmountAll(this)


  override def graph: CAPSGraph = CAPSGraph.empty(this)

  override def cypher(graph: Graph, query: String, queryParameters: Map[String, CypherValue]): Result = {
    val (stmt, extractedLiterals, semState) = parser.process(query)(CypherParser.defaultContext)

    val globals = GlobalsExtractor(stmt, GlobalsRegistry.fromTokens(graph.tokens.registry))
    val GlobalsRegistry(tokens, constants) = globals

    val converted = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = (queryParameters ++ converted).collect {
      case (k, v) if constants.contains(k) => constants.constantRefByName(k) -> v
    }

    val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

    print("IR ... ")
    val ir = IRBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, semState, paramsAndTypes))
    println("Done!")

    print("Logical plan ... ")
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, sourceAt)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    println("Done!")

    print("Optimizing logical plan ... ")
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    println("Done!")

    if (PrintLogicalPlan.get())
      println(optimizedLogicalPlan.pretty())

    // TODO: Initialize with a table with no columns and a single row
    plan(graph, CAPSRecords.empty()(this), tokens, constants, allParameters, optimizedLogicalPlan)
  }

  def filter(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records = {
    val scan = producer.planStart(graph.schema, in.header.fields)
    val filter = producer.planFilter(expr, scan)
    plan(graph, in, queryParameters, filter).records
  }

  def select(graph: Graph, in: Records, fields: IndexedSeq[Var], queryParameters: Map[String, CypherValue]): Records = {
    val scan = producer.planStart(graph.schema, in.header.fields)
    val select = producer.planSelect(fields, scan)
    plan(graph, in, queryParameters, select).records
  }

  def project(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records = {
    val scan = producer.planStart(graph.schema, in.header.fields)
    val project = producer.projectExpr(expr, scan)
    plan(graph, in, queryParameters, project).records
  }

  def alias(graph: Graph, in: Records, alias: (Expr, Var), queryParameters: Map[String, CypherValue]): Records = {
    val (expr, v) = alias
    val scan = producer.planStart(graph.schema, in.header.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(graph: CAPSGraph,
                   records: CAPSRecords,
                   queryParameters: Map[String, CypherValue],
                   logicalPlan: LogicalOperator): CAPSResult = {

    val globals = GlobalsRegistry.fromTokens(graph.tokens.registry)
    val allParameters = queryParameters.map { case (k, v) => globals.constants.constantRefByName(k) -> v }

    plan(graph, records, globals.tokens, globals.constants, allParameters, logicalPlan)
  }

  private def plan(graph: CAPSGraph,
                   records: CAPSRecords,
                   tokens: TokenRegistry,
                   constants: ConstantRegistry,
                   allParameters: Map[ConstantRef, CypherValue],
                   logicalPlan: LogicalOperator): CAPSResult = {
    // TODO: Remove dependency on globals (?) Only needed to enforce everything is known, that could be done
    //       differently
    print("Flat plan ... ")
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(tokens, constants))
    println("Done!")

    // TODO: It may be better to pass tokens around in the physical planner explicitly (via the records)
    //       instead of just using a single global tokens instance derived from the graph space
    //
    print("Physical plan ... ")
    val physicalPlannerContext = PhysicalPlannerContext(graph, records, tokens, constants, allParameters)
    val physicalResult = physicalPlanner(flatPlan)(physicalPlannerContext)
    println("Done!")

    CAPSResultBuilder.from(physicalResult)
  }
}

object CAPSSession extends Serializable {

  def create(implicit session: SparkSession): CAPSSession = Builder(session).build

  case class Builder(session: SparkSession,
                     private val graphSourceFactories: Set[CAPSGraphSourceFactory] = Set.empty) {

    def withGraphSourceFactory(factory: CAPSGraphSourceFactory): Builder =
      copy(graphSourceFactories = graphSourceFactories + factory)

    def build: CAPSSession = {
      val sessionFactory = SessionGraphSourceFactory()
      // add some default factories
      val additionalFactories = graphSourceFactories +
        Neo4jGraphSourceFactory() +
        HdfsCsvGraphSourceFactory(session.sparkContext.hadoopConfiguration)

      new CAPSSession(
        session,
        CAPSGraphSourceHandler(sessionFactory, additionalFactories)
      )
    }
  }

  def builder(sparkSession: SparkSession): Builder = Builder(sparkSession)
}
