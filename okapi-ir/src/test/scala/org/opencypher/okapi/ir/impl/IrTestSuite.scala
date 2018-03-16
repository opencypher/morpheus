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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.io._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.test.BaseTestSuite
import org.scalatest.mockito.MockitoSugar

import scala.language.implicitConversions

abstract class IrTestSuite extends BaseTestSuite with MockitoSugar {

  val testNamespace = Namespace("testNamespace")
  val testGraphName = GraphName("test")
  val testGraphSchema = Schema.empty
  val testQualifiedGraphName = QualifiedGraphName(testNamespace, testGraphName)

  def testGraph()(implicit schema: Schema = testGraphSchema) =
    IRCatalogGraph(testQualifiedGraphName, schema)

  def testGraphSource(graphsWithSchema: (GraphName, Schema)*): PropertyGraphDataSource = {
    val gs = mock[PropertyGraphDataSource]
    graphsWithSchema.foreach {
      case (graphName, schema) => when(gs.schema(graphName)).thenReturn(Some(schema))
    }
    gs
  }

  def leafBlock(): SourceBlock[Expr] = SourceBlock[Expr](testGraph)

  val graphBlock: SourceBlock[Expr] = SourceBlock[Expr](testGraph)

  def project(
    fields: Fields[Expr],
    after: List[Block[Expr]] = List(leafBlock),
    given: List[Expr] = List.empty[Expr]) =
    ProjectBlock(after, fields, given, testGraph)

  protected def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](List(leafBlock), pattern, List.empty[Expr], false, testGraph)

  def irFor(root: Block[Expr], blocks: List[Block[Expr]] = List.empty): CypherQuery[Expr] = {
    val result = TableResultBlock[Expr](
      after = List(root),
      binds = OrderedFields[Expr](),
      graph = testGraph
    )
    val model = QueryModel(result, CypherMap.empty)
    CypherQuery(QueryInfo("test"), model)
  }

  case class DummyBlock[E](override val after: List[Block[E]] = List.empty) extends BasicBlock[DummyBinds[E], E](BlockType("dummy")) {
    override def binds: DummyBinds[E] = DummyBinds[E]()

    override def where: List[E] = List.empty[E]

    override val graph = testGraph
  }

  case class DummyBinds[E](fields: Set[IRField] = Set.empty) extends Binds[E]

  implicit class RichString(queryText: String) {
    def model: QueryModel[Expr] = ir().model

    def ir(graphsWithSchema: (GraphName, Schema)*)(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      val parameters = Map.empty[String, CypherValue]
      IRBuilder(stmt)(
        IRBuilderContext.initial(queryText,
          parameters,
          SemanticState.clean,
          testGraph()(schema),
          _ => testGraphSource(graphsWithSchema :+ (testGraphName -> schema): _*)))
    }

    def irWithParams(params: (String, CypherValue)*)(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      IRBuilder(stmt)(
        IRBuilderContext.initial(queryText,
          params.toMap,
          SemanticState.clean,
          testGraph()(schema),
          _ => testGraphSource(testGraphName -> schema)))
    }
  }

}
