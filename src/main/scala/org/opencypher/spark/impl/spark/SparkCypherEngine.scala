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
package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.global.{ConstantRef, ConstantRegistry, GlobalsRegistry, TokenRegistry}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.spark.impl.ir.global.GlobalsExtractor
import org.opencypher.spark.impl.ir.{CypherQueryBuilder, IRBuilderContext}
import org.opencypher.spark.impl.logical._
import org.opencypher.spark.impl.parse.CypherParser
import org.opencypher.spark.impl.physical.{PhysicalPlanner, PhysicalPlannerContext, SparkCypherResultBuilder}

import scala.util.Try

final class SparkCypherEngine extends Cypher with Serializable {

  override type Graph = SparkCypherGraph
  override type Space = SparkGraphSpace
  override type Records = SparkCypherRecords
  override type Result = SparkCypherResult
  override type Data = DataFrame

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner()
  private val parser = CypherParser

  override def cypher(graph: Graph, query: String, queryParameters: Map[String, CypherValue]): Result = {
    val (stmt, extractedLiterals) = parser.process(query)(CypherParser.defaultContext)

    val globals = GlobalsExtractor(stmt, GlobalsRegistry(graph.space.tokens.registry))
    val GlobalsRegistry(tokens, constants) = globals

    val converted = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = (queryParameters ++ converted).collect {
      case (k, v) if Try(constants.constantRefByName(k)).isSuccess => constants.constantRefByName(k) -> v
    }

    val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

    print("IR ... ")
    val ir = CypherQueryBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, paramsAndTypes))
    println("Done!")

    print("Logical plan ... ")
    val logicalPlan = logicalPlanner(ir)(LogicalPlannerContext(graph.schema, Set.empty))
    println("Done!")

    plan(graph, SparkCypherRecords.empty()(graph.space), tokens, constants, allParameters, logicalPlan)
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

  private def plan(graph: SparkCypherGraph,
                   records: SparkCypherRecords,
                   queryParameters: Map[String, CypherValue],
                   logicalPlan: LogicalOperator): SparkCypherResult = {

    val globals = GlobalsRegistry(graph.space.tokens.registry)
    val allParameters = queryParameters.map { case (k, v) => globals.constants.constantRefByName(k) -> v }

    plan(graph, records, globals.tokens, globals.constants, allParameters, logicalPlan)
  }

  private def plan(graph: SparkCypherGraph,
                   records: SparkCypherRecords,
                   tokens: TokenRegistry,
                   constants: ConstantRegistry,
                   allParameters: Map[ConstantRef, CypherValue],
                   logicalPlan: LogicalOperator): SparkCypherResult = {
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

    SparkCypherResultBuilder.from(physicalResult)
  }
}
