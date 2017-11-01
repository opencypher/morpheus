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
package org.opencypher.caps.ir.impl

import java.net.URI

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticState
import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.io.CAPSGraphSource
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.logical.{LogicalExternalGraph, Start}
import org.opencypher.caps.impl.parse.CypherParser
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block._
import org.opencypher.caps.ir.api.pattern.{AllGiven, Pattern}
import org.opencypher.caps.test.BaseTestSuite
import org.scalatest.mockito.MockitoSugar

import scala.language.implicitConversions

abstract class IrTestSuite extends BaseTestSuite with MockitoSugar {
  val leafRef = BlockRef("leaf")

  def testGraph()(implicit schema: Schema = Schema.empty) =
    IRExternalGraph("test", schema, URI.create("test"))

  val testGraphSource: CAPSGraphSource = mock[CAPSGraphSource]
  when(testGraphSource.schema).thenReturn(Some(testGraph.schema))

  def leafBlock(): SourceBlock[Expr] = SourceBlock[Expr](testGraph)
  def leafPlan: Start =
    Start(LogicalExternalGraph(testGraph.name, testGraph.uri, testGraph.schema), Set.empty)(
      SolvedQueryModel.empty)

  val graphBlockRef: BlockRef       = BlockRef("graph")
  val graphBlock: SourceBlock[Expr] = SourceBlock[Expr](testGraph)

  /**
    * Construct a single-block ir; the parameter block has to be a block that could be planned as a leaf.
    */
  def irFor(leaf: Block[Expr]): CypherQuery[Expr] =
    irFor(BlockRef("root"), Map(BlockRef("root") -> leaf))

  /**
    * Construct a two-block ir; the parameter block needs have the leafRef in its after set.
    * A leaf block will be created.
    */
  def irWithLeaf(nonLeaf: Block[Expr]): CypherQuery[Expr] = {
    val rootRef = BlockRef("root")
    val blocks  = Map(rootRef -> nonLeaf, BlockRef("nonLeaf") -> nonLeaf, leafRef -> leafBlock)
    irFor(rootRef, blocks)
  }

  def project(fields: FieldsAndGraphs[Expr],
              after: Set[BlockRef] = Set(leafRef),
              given: AllGiven[Expr] = AllGiven[Expr]()) =
    ProjectBlock(after, fields, given, testGraph)

  protected def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](Set(leafRef), pattern, AllGiven[Expr](), false, testGraph)

  def irFor(rootRef: BlockRef, blocks: Map[BlockRef, Block[Expr]]): CypherQuery[Expr] = {
    val result = ResultBlock[Expr](
      after = Set(rootRef),
      binds = OrderedFieldsAndGraphs[Expr](),
      nodes = Set.empty,
      relationships = Set.empty,
      where = AllGiven[Expr](),
      source = testGraph
    )
    val model = QueryModel(result, Map.empty, blocks, Map.empty)
    CypherQuery(QueryInfo("test"), model)
  }

  case class DummyBlock[E](after: Set[BlockRef] = Set.empty)
      extends BasicBlock[DummyBinds[E], E](BlockType("dummy")) {
    override def binds: DummyBinds[E] = DummyBinds[E]()
    override def where: AllGiven[E]   = AllGiven[E]()
    override val source               = testGraph
  }

  case class DummyBinds[E](fields: Set[IRField] = Set.empty) extends Binds[E]

  implicit class RichString(queryText: String) {
    def model: QueryModel[Expr] = ir.model

    def ir(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt       = CypherParser(queryText)(CypherParser.defaultContext)
      val parameters = Map.empty[String, CypherValue]
      IRBuilder(stmt)(
        IRBuilderContext
          .initial(queryText, parameters, SemanticState.clean, testGraph, _ => testGraphSource))
    }

    def irWithParams(params: (String, CypherValue)*)(
        implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      IRBuilder(stmt)(
        IRBuilderContext
          .initial(queryText, params.toMap, SemanticState.clean, testGraph, _ => testGraphSource))
    }
  }
}
