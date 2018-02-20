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

import java.util.UUID

import org.opencypher.caps.api.configuration.CoraConfiguration.PrintFlatPlan
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.{GraphName, QualifiedGraphName}
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.cosc.impl.COSCConverters._
import org.opencypher.caps.cosc.impl.planning.{COSCPhysicalOperatorProducer, COSCPhysicalPlannerContext}
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.physical.PhysicalPlanner
import org.opencypher.caps.impl.util.Measurement.time
import org.opencypher.caps.ir.api.IRExternalGraphNew
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.caps.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner, LogicalPlannerContext}

object COSCSession {
  def create: COSCSession = new COSCSession()
}

class COSCSession() extends CypherSession {

  self =>

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner(new COSCPhysicalOperatorProducer()(this))
  private val parser = CypherParser

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
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, dataSource)))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.isSet) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
    if (PrintFlatPlan.isSet) println(flatPlan.pretty)

    val coscPlannerContext = COSCPhysicalPlannerContext(this, readFrom, COSCRecords.unit()(self), allParameters)
    val coscPlan = time("Physical planning")(physicalPlanner.process(flatPlan)(coscPlannerContext))

    time("Query execution")(COSCResultBuilder.from(logicalPlan, flatPlan, coscPlan)(COSCRuntimeContext(coscPlannerContext.parameters, graphAt)))
  }

  private def graphAt(qualifiedGraphName: QualifiedGraphName): Option[COSCGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asCosc)

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraphNew = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = mount(graphName, ambient)
    IRExternalGraphNew(graphName.value, ambient.schema, qualifiedGraphName)
  }
}
