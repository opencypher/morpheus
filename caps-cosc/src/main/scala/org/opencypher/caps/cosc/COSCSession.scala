package org.opencypher.caps.cosc

import java.net.URI
import java.util.UUID

import org.opencypher.caps.api.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.{PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.value.{CAPSValue, CypherValue}
import org.opencypher.caps.cosc.Configuration.PrintCOSCPlan
import org.opencypher.caps.cosc.datasource.{COSCGraphSourceHandler, COSCPropertyGraphDataSource, COSCSessionPropertyGraphDataSourceFactory}
import org.opencypher.caps.cosc.planning.{COSCPlanner, COSCPlannerContext}
import org.opencypher.caps.demo.Configuration.{PrintFlatPlan, PrintLogicalPlan}
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.util.Measurement
import org.opencypher.caps.impl.util.Measurement.time
import org.opencypher.caps.ir.api.IRExternalGraph
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner, LogicalPlannerContext}

object COSCSession {
  def create: COSCSession = new COSCSession(COSCGraphSourceHandler(COSCSessionPropertyGraphDataSourceFactory(), Set.empty))
}

class COSCSession(private val graphSourceHandler: COSCGraphSourceHandler) extends CypherSession {

  self =>

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val coscPlanner = new COSCPlanner()
  private val parser = CypherParser

  def sourceAt(uri: URI): PropertyGraphDataSource = graphSourceHandler.sourceAt(uri)(this)

  def optGraphAt(uri: URI): Option[PropertyGraph] = graphSourceHandler.optSourceAt(uri)(this).map(_.graph)

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: Map[String, CypherValue]): CypherResult =
    cypherOnGraph(COSCGraph.empty(this), query, parameters)

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: Map[String, CypherValue] = Map.empty): CypherResult = {
    val ambientGraph = getAmbientGraph(graph)

    println(
      s"""Executing query:
         |$query
       """.stripMargin)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters = extractedLiterals.mapValues(v => CAPSValue(v))
    val allParameters = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, sourceAt)))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.andThen(sourceAt), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.get()) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
    if (PrintFlatPlan.get()) println(flatPlan.pretty)

    val coscPlannerContext = COSCPlannerContext(readFrom, COSCRecords.unit, allParameters)
    val coscPlan = time("COSC planning")(coscPlanner.process(flatPlan)(coscPlannerContext))
    if (PrintCOSCPlan.get()) println(coscPlan.pretty)

    time("Query execution")(COSCResultBuilder.from(logicalPlan, flatPlan, coscPlan)(COSCRuntimeContext(coscPlannerContext.parameters, optGraphAt)))
  }

  /**
    * Reads a graph from the argument URI.
    *
    * @param uri URI locating a graph
    * @return graph located at the URI
    */
  override def readFrom(uri: URI): PropertyGraph =
    graphSourceHandler.sourceAt(uri)(this).graph

  /**
    * Mounts the given graph source to session-local storage under the given path. The specified graph will be
    * accessible under the session-local URI scheme, e.g. {{{session://$path}}}.
    *
    * @param source graph source to register
    * @param path   path at which this graph can be accessed via {{{session://$path}}}
    */
  override def mount(source: PropertyGraphDataSource, path: String): Unit = ???

  /**
    * Writes the given graph to the location using the format specified by the URI.
    *
    * @param graph graph to write
    * @param uri   graph URI indicating location and format to write the graph to
    * @param mode  persist mode which determines what happens if the location is occupied
    */
  override def write(graph: PropertyGraph, uri: String, mode: PersistMode): Unit = ???

  private def getAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val name = UUID.randomUUID().toString
    val uri = URI.create(s"session:///graphs/ambient/$name")

    val graphSource = new COSCPropertyGraphDataSource {
      override def schema: Option[Schema] = Some(graph.schema)

      override def canonicalURI: URI = uri

      override def delete(): Unit = throw UnsupportedOperationException("Deletion of an ambient graph")

      override def graph: PropertyGraph = ambient

      override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI

      override def create: COSCGraph = throw UnsupportedOperationException("Creation of an ambient graph")

      override def store(graph: PropertyGraph, mode: PersistMode): COSCGraph =
        throw UnsupportedOperationException("Persisting an ambient graph")

      override val session: COSCSession = self
    }

    graphSourceHandler.mountSourceAt(graphSource, uri)(self)

    IRExternalGraph(name, ambient.schema, uri)
  }
}
