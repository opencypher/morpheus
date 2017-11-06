/*
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
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.caps.api.expr.{Expr, Var}
import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.api.io.{CreateOrFail, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.io.{CAPSGraphSource, CAPSGraphSourceFactory}
import org.opencypher.caps.api.util.parsePathOrURI
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.demo.Configuration.{PrintLogicalPlan, PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.caps.demo.CypherKryoRegistrar
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.logical._
import org.opencypher.caps.impl.parse.CypherParser
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceHandler
import org.opencypher.caps.impl.spark.io.file.FileCsvGraphSourceFactory
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvGraphSourceFactory
import org.opencypher.caps.impl.spark.io.neo4j.Neo4jGraphSourceFactory
import org.opencypher.caps.impl.spark.io.session.SessionGraphSourceFactory
import org.opencypher.caps.impl.spark.physical.{CAPSResultBuilder, PhysicalPlanner, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.caps.ir.api.{IRExternalGraph, IRField}
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}

sealed class CAPSSession private(val sparkSession: SparkSession,
                                 private val graphSourceHandler: CAPSGraphSourceHandler)
  extends CypherSession with Serializable {

  self =>

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
  private val temporaryColumnId = new AtomicLong()

  def temporaryColumnName(): String = {
    s"___Tmp${temporaryColumnId.incrementAndGet()}"
  }

  def sourceAt(uri: URI): CAPSGraphSource =
    graphSourceHandler.sourceAt(uri)(this)

  def graphAt(path: String): CAPSGraph =
    graphAt(parsePathOrURI(path))

  def graphAt(uri: URI): CAPSGraph =
    graphSourceHandler.sourceAt(uri)(this).graph

  def optGraphAt(uri: URI): Option[CAPSGraph] =
    graphSourceHandler.optSourceAt(uri)(this).map(_.graph)

  def storeGraphAt(graph: CAPSGraph, pathOrUri: String, mode: PersistMode = CreateOrFail): CAPSGraph =
    graphSourceHandler.sourceAt(parsePathOrURI(pathOrUri))(this).store(graph, mode)

  def mountSourceAt(source: CAPSGraphSource, pathOrUri: String): Unit =
    mountSourceAt(source, parsePathOrURI(pathOrUri))

  def mountSourceAt(source: CAPSGraphSource, uri: URI): Unit =
    graphSourceHandler.mountSourceAt(source, uri)(this)

  def unmountAll(): Unit =
    graphSourceHandler.unmountAll(this)

  override def emptyGraph: CAPSGraph = CAPSGraph.empty(this)

  override def cypher(graph: Graph, query: String, queryParameters: Map[String, CypherValue]): Result = {
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = parser.process(query)(CypherParser.defaultContext)

    val extractedParameters = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR ...", false)
    val ir = IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, sourceAt))
    logStageProgress("Done!")

    logStageProgress("Logical plan ...", false)
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.andThen(sourceAt))
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

  private def mountAmbientGraph(ambient: CAPSGraph): IRExternalGraph = {
    val name = UUID.randomUUID().toString
    val uri = URI.create(s"session:///graphs/ambient/$name")

    val graphSource = new CAPSGraphSource {
      override def schema: Option[Schema] = Some(graph.schema)
      override def canonicalURI: URI = uri
      override def delete(): Unit = Raise.impossible("Don't delete the ambient graph")
      override def graph: CAPSGraph = ambient
      override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI
      override def create: CAPSGraph = Raise.impossible("Don't create the ambient graph")
      override def store(graph: CAPSGraph, mode: PersistMode): CAPSGraph = Raise.impossible("Don't persist the ambient graph")
      override val session: CAPSSession = self
    }

    graphSourceHandler.mountSourceAt(graphSource, uri)(self)

    IRExternalGraph(name, ambient.schema, uri)
  }

  private def planStart(graph: Graph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalExternalGraph(ambientGraph.name, ambientGraph.uri, graph.schema), fields)
  }

  def filter(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records = {
    val scan = planStart(graph, in.header.fields)
    val filter = producer.planFilter(expr, scan)
    plan(graph, in, queryParameters, filter).records
  }

  def select(graph: Graph, in: Records, fields: IndexedSeq[Var], queryParameters: Map[String, CypherValue]): Records = {
    val scan = planStart(graph, in.header.fields)
    val select = producer.planSelect(fields, Set.empty, scan)
    plan(graph, in, queryParameters, select).records
  }

  def project(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records = {
    val scan = planStart(graph, in.header.fields)
    val project = producer.projectExpr(expr, scan)
    plan(graph, in, queryParameters, project).records
  }

  def alias(graph: Graph, in: Records, alias: (Expr, Var), queryParameters: Map[String, CypherValue]): Records = {
    val (expr, v) = alias
    val scan = planStart(graph, in.header.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(graph: CAPSGraph,
                   records: CAPSRecords,
                   parameters: Map[String, CypherValue],
                   logicalPlan: LogicalOperator): CAPSResult = {
    logStageProgress("Flat plan ... ", false)
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(parameters))
    logStageProgress("Done!")

    logStageProgress("Physical plan ... ", false)
    val physicalPlannerContext = PhysicalPlannerContext(graphAt, records, parameters)
    val physicalPlan = physicalPlanner(flatPlan)(physicalPlannerContext)
    logStageProgress("Done!")

    if (PrintPhysicalPlan.get()) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }

    CAPSResultBuilder.from(physicalPlan, logicalPlan)(RuntimeContext(physicalPlannerContext.parameters, optGraphAt))
  }

  override def toString: String = {
    val mountPoints = graphSourceHandler.sessionGraphSourceFactory.mountPoints.keys

    val mountPointsString = if (mountPoints.nonEmpty)
      s"mountPoints: ${mountPoints.mkString(", ")}"
    else "No graphs mounted"

    s"${this.getClass.getSimpleName}($mountPointsString)"
  }
}

object CAPSSession extends Serializable {

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.kryo.unsafe", "true")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.set("spark.kryo.registrationRequired", "true")

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

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
        HdfsCsvGraphSourceFactory(session.sparkContext.hadoopConfiguration) +
        FileCsvGraphSourceFactory()

      new CAPSSession(
        session,
        CAPSGraphSourceHandler(sessionFactory, additionalFactories)
      )
    }
  }

  def builder(sparkSession: SparkSession): Builder = Builder(sparkSession)
}
