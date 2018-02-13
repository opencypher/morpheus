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
package org.opencypher.caps.cosc.impl

import java.net.URI
import java.util.UUID

import org.opencypher.caps.api.configuration.CoraConfiguration.PrintFlatPlan
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.{PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.cosc.impl.COSCConverters._
import org.opencypher.caps.cosc.impl.datasource.{COSCGraphSourceHandler, COSCPropertyGraphDataSource, COSCSessionPropertyGraphDataSourceFactory}
import org.opencypher.caps.cosc.impl.planning.{COSCPhysicalOperatorProducer, COSCPhysicalPlannerContext}
import org.opencypher.caps.impl.exception.UnsupportedOperationException
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.physical.PhysicalPlanner
import org.opencypher.caps.impl.record.CypherRecords
import org.opencypher.caps.impl.util.Measurement.time
import org.opencypher.caps.ir.api.IRExternalGraph
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
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
  private val physicalPlanner = new PhysicalPlanner(new COSCPhysicalOperatorProducer()(this))
  private val parser = CypherParser

  def sourceAt(uri: URI): PropertyGraphDataSource =
    graphSourceHandler.sourceAt(uri)(this)

  def optGraphAt(uri: URI): Option[COSCGraph] =
    graphSourceHandler.optSourceAt(uri)(this).map(_.graph) match {
      case Some(graph) => Some(graph.asCosc)
      case None => None
    }

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords] = None): CypherResult =
    cypherOnGraph(COSCGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraph = getAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, sourceAt)))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.andThen(sourceAt), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.get) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
    if (PrintFlatPlan.get) println(flatPlan.pretty)

    val coscPlannerContext = COSCPhysicalPlannerContext(this, readFrom, COSCRecords.unit()(self), allParameters)
    val coscPlan = time("Physical planning")(physicalPlanner.process(flatPlan)(coscPlannerContext))

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
