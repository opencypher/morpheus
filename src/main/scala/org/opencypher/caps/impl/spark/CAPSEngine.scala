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
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.caps.api.classes.Cypher
import org.opencypher.caps.api.expr.{Expr, Var}
import org.opencypher.caps.api.ir.Field
import org.opencypher.caps.api.ir.global.{ConstantRef, ConstantRegistry, GlobalsRegistry, TokenRegistry}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult, SparkGraphSpace}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.ir.global.GlobalsExtractor
import org.opencypher.caps.impl.ir.{CypherQueryBuilder, IRBuilderContext}
import org.opencypher.caps.impl.logical._
import org.opencypher.caps.impl.parse.CypherParser
import org.opencypher.caps.impl.physical.{PhysicalPlanner, PhysicalPlannerContext, CAPSResultBuilder}

final class CAPSEngine extends Cypher with Serializable {

  override type Graph = CAPSGraph
  override type Space = SparkGraphSpace
  override type Records = CAPSRecords
  override type Result = CAPSResult
  override type Data = DataFrame

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = new LogicalOptimizer(producer)
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner()
  private val parser = CypherParser

  override def cypher(graph: Graph, query: String, queryParameters: Map[String, CypherValue]): Result = {
    val (stmt, extractedLiterals) = parser.process(query)(CypherParser.defaultContext)

    val globals = GlobalsExtractor(stmt, GlobalsRegistry(graph.space.tokens.registry))
    val GlobalsRegistry(tokens, constants) = globals

    val converted = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = (queryParameters ++ converted).collect {
      case (k, v) if constants.contains(k) => constants.constantRefByName(k) -> v
    }

    val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

    print("IR ... ")
    val ir = CypherQueryBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, paramsAndTypes))
    println("Done!")

    print("Logical plan ... ")
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    println("Done!")

    print("Optimizing logical plan ... ")
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    println("Done!")

    plan(graph, CAPSRecords.empty()(graph.space), tokens, constants, allParameters, optimizedLogicalPlan)
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

  def alias(graph: Graph, in: Records, alias: (Expr, Var),  queryParameters: Map[String, CypherValue]): Records = {
    val (expr, v) = alias
    val scan = producer.planStart(graph.schema, in.header.fields)
    val select = producer.projectField(Field(v.name)(v.cypherType), expr, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(graph: CAPSGraph,
                   records: CAPSRecords,
                   queryParameters: Map[String, CypherValue],
                   logicalPlan: LogicalOperator): CAPSResult = {

    val globals = GlobalsRegistry(graph.space.tokens.registry)
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
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(graph.schema, tokens, constants))
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
