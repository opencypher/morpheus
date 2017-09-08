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
package org.opencypher.caps.impl.ir

import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Parameter}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticState, symbols}
import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.api.ir._
import org.opencypher.caps.api.ir.block._
import org.opencypher.caps.api.ir.global.GlobalsRegistry
import org.opencypher.caps.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.impl.ir.global.GlobalsExtractor
import org.opencypher.caps.impl.logical.{DefaultGraphSource, NamedLogicalGraph, Start}
import org.opencypher.caps.impl.parse.CypherParser
import org.opencypher.caps.test.BaseTestSuite

import scala.language.implicitConversions

abstract class IrTestSuite extends BaseTestSuite {
  val leafRef = BlockRef("leaf")
  val leafBlock = LoadGraphBlock[Expr](Set.empty, DefaultGraph())
  val leafPlan = Start(NamedLogicalGraph("default", Schema.empty), DefaultGraphSource, Set.empty)(SolvedQueryModel.empty)

  val graphBlockRef = BlockRef("graph")
  val graphBlock = LoadGraphBlock[Expr](Set.empty, DefaultGraph())

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
    val blocks = Map(rootRef -> nonLeaf, BlockRef("nonLeaf") -> nonLeaf, leafRef -> leafBlock)
    irFor(rootRef, blocks)
  }

  def project(fields: ProjectedFields[Expr], after: Set[BlockRef] = Set(leafRef),
              given: AllGiven[Expr] = AllGiven[Expr]()) =
    ProjectBlock(after, fields, given, graphBlockRef)

  protected def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](Set(leafRef), pattern, AllGiven[Expr](), false, graphBlockRef)

  def irFor(rootRef: BlockRef, blocks: Map[BlockRef, Block[Expr]]): CypherQuery[Expr] = {
    val result = ResultBlock[Expr](
      after = Set(rootRef),
      // TODO
      binds = OrderedFields[Expr](),
      nodes = Set.empty, // TODO: Fill these sets correctly
      relationships = Set.empty,
      where = AllGiven[Expr](),
      source = graphBlockRef
    )
    val model = QueryModel(result, GlobalsRegistry.empty, blocks, Map(graphBlockRef -> Schema.empty))
    CypherQuery(QueryInfo("test"), model)
  }

  case class DummyBlock[E](after: Set[BlockRef] = Set.empty) extends BasicBlock[DummyBinds[E], E](BlockType("dummy")) {
    override def binds: DummyBinds[E] = DummyBinds[E]()
    override def where: AllGiven[E] = AllGiven[E]()
    override val source = graphBlockRef
  }

  case class DummyBinds[E](fields: Set[IRField] = Set.empty) extends Binds[E]

  implicit class RichString(queryText: String) {
    def model: QueryModel[Expr] = ir.model

    // TODO: SemCheck
    def ir(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      IRBuilder(stmt)(IRBuilderContext.initial(queryText, GlobalsExtractor(stmt), schema, SemanticState.clean, Map.empty))
    }

    // TODO: SemCheck
    def irWithParams(params: (String, CypherType)*)(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      val knownTypes: Map[Expression, CypherType] = params.map(p => Parameter(p._1, symbols.CTAny)(InputPosition.NONE) -> p._2).toMap
      IRBuilder(stmt)(IRBuilderContext.initial(queryText, GlobalsExtractor(stmt), schema, SemanticState.clean, knownTypes))
    }
  }
}
