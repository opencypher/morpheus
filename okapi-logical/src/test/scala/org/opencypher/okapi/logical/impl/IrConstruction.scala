package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.v9_1.ast.semantics.SemanticState

import scala.reflect.ClassTag

trait IrConstruction {

  self: BaseTestSuite =>

  def project(
    fields: Fields[Expr],
    after: List[Block[Expr]] = List(leafBlock),
    given: Set[Expr] = Set.empty[Expr]) =
    ProjectBlock(after, fields, given, testGraph)


  private def testGraph()(implicit schema: Schema = testGraphSchema) =
    IRCatalogGraph(testQualifiedGraphName, schema)

  protected def leafPlan: Start =
    Start(LogicalCatalogGraph(testGraph.qualifiedGraphName, testGraph.schema), SolvedQueryModel.empty)

  protected def irFor(root: Block[Expr]): CypherQuery[Expr] = {
    val result = TableResultBlock[Expr](
      after = List(root),
      binds = OrderedFields[Expr](),
      graph = testGraph
    )
    val model = QueryModel(result, CypherMap.empty)
    CypherQuery(QueryInfo("test"), model)
  }

  protected def leafBlock: SourceBlock[Expr] = SourceBlock[Expr](testGraph)

  protected def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](List(leafBlock), pattern, Set.empty[Expr], false, testGraph)

  implicit class RichString(queryText: String) {
    def parseIR[T <: CypherStatement[Expr] : ClassTag](graphsWithSchema: (GraphName, Schema)*)(implicit schema: Schema = Schema.empty): T =
      ir(graphsWithSchema:_ *) match {
        case cq : T => cq
        case other => throw new IllegalArgumentException(s"Cannot convert $other")
      }

    def asCypherQuery(graphsWithSchema: (GraphName, Schema)*)(implicit schema: Schema = Schema.empty): CypherQuery[Expr] =
      parseIR[CypherQuery[Expr]](graphsWithSchema: _*)

    def ir(graphsWithSchema: (GraphName, Schema)*)(implicit schema: Schema = Schema.empty): CypherStatement[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      val parameters = Map.empty[String, CypherValue]
      IRBuilder(stmt)(
        IRBuilderContext.initial(
          queryText,
          parameters,
          SemanticState.clean,
          testGraph()(schema),
          qgnGenerator,
          Map.empty.withDefaultValue(testGraphSource(graphsWithSchema :+ (testGraphName -> schema): _*))
        ))
    }

    def irWithParams(params: (String, CypherValue)*)(implicit schema: Schema = Schema.empty): CypherStatement[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      IRBuilder(stmt)(
        IRBuilderContext.initial(queryText,
          params.toMap,
          SemanticState.clean,
          testGraph()(schema),
          qgnGenerator,
          Map.empty.withDefaultValue(testGraphSource(testGraphName -> schema))))
    }
  }


}
