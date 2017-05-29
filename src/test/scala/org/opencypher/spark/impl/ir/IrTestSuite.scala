package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Parameter}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, symbols}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.expr.Expr
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.logical.{IDontCareGraph, LoadGraph, NamedLogicalGraph}
import org.opencypher.spark.impl.parse.CypherParser

import scala.language.implicitConversions

abstract class IrTestSuite extends StdTestSuite {
  val leafRef = BlockRef("leaf")
  val leafBlock = LoadGraphBlock[Expr](Set.empty, DefaultGraph())
  val leafPlan = LoadGraph(IDontCareGraph, NamedLogicalGraph("default"))(SolvedQueryModel.empty)

  val graphBlockRef = BlockRef("graph")
  val graphBlock = LoadGraphBlock[Expr](Set.empty, DefaultGraph())

  implicit def toField(s: Symbol): Field = Field(s.name)()
  def toField(s: Symbol, t: CypherType): Field = Field(s.name)(t)

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
    MatchBlock[Expr](Set(leafRef), pattern, AllGiven[Expr](), graphBlockRef)

  def irFor(rootRef: BlockRef, blocks: Map[BlockRef, Block[Expr]]): CypherQuery[Expr] = {
    val result = ResultBlock[Expr](
      after = Set(rootRef),
      // TODO
      binds = OrderedFields[Expr](),
      nodes = Set.empty, // TODO: Fill these sets correctly
      relationships = Set.empty,
      where = AllGiven[Expr](),
      graph = graphBlockRef
    )
    val model = QueryModel(result, GlobalsRegistry.none, blocks, Map(graphBlockRef -> Schema.empty))
    CypherQuery(QueryInfo("test"), model)
  }

  case class DummyBlock[E](after: Set[BlockRef] = Set.empty) extends BasicBlock[DummyBinds[E], E](BlockType("dummy")) {
    override def binds: DummyBinds[E] = DummyBinds[E]()
    override def where: AllGiven[E] = AllGiven[E]()
    override val graph = graphBlockRef
  }

  case class DummyBinds[E](fields: Set[Field] = Set.empty) extends Binds[E]

  implicit class RichString(queryText: String) {
    def model: QueryModel[Expr] = ir.model

    def ir(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      CypherQueryBuilder(stmt)(IRBuilderContext.initial(queryText, GlobalsExtractor(stmt), schema, Map.empty))
    }

    def irWithParams(params: (String, CypherType)*)(implicit schema: Schema = Schema.empty): CypherQuery[Expr] = {
      val stmt = CypherParser(queryText)(CypherParser.defaultContext)
      val knownTypes: Map[Expression, CypherType] = params.map(p => Parameter(p._1, symbols.CTAny)(InputPosition.NONE) -> p._2).toMap
      CypherQueryBuilder(stmt)(IRBuilderContext.initial(queryText, GlobalsExtractor(stmt), schema, knownTypes))
    }
  }
}
