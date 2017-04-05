package org.opencypher.spark.impl.ir

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern.{AllGiven, EveryNode, Pattern}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.impl.logical.NodeScan
import org.opencypher.spark.impl.parse.CypherParser

import scala.language.implicitConversions

abstract class IrTestSuite extends StdTestSuite {
  val leafRef = BlockRef("leaf")
  val leafBlock = matchBlock(Pattern.empty[Expr].withEntity('n, EveryNode))
  val emptySig = RecordHeader.empty
  val leafPlan = NodeScan('n, EveryNode, emptySig)(SolvedQueryModel.empty)

  implicit def toField(s: Symbol): Field = Field(s.name)()
  implicit def toVar(s: Symbol): Var = Var(s.name)

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
    ProjectBlock(after, fields, given)

  protected def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](Set.empty, pattern, AllGiven[Expr]())

  def irFor(rootRef: BlockRef, blocks: Map[BlockRef, Block[Expr]]): CypherQuery[Expr] = {
    val result = ResultBlock[Expr](
      after = Set(rootRef),
      // TODO
      binds = OrderedFields[Expr](),
      nodes = Set.empty, // TODO: Fill these sets correctly
      relationships = Set.empty,
      where = AllGiven[Expr]()
    )
    val model = QueryModel(result, GlobalsRegistry.none, blocks)
    CypherQuery(QueryInfo("test"), model)
  }

  case class DummyBlock[E](after: Set[BlockRef] = Set.empty) extends BasicBlock[DummyBinds[E], E](BlockType("dummy")) {
    override def binds: DummyBinds[E] = DummyBinds[E]()
    override def where: AllGiven[E] = AllGiven[E]()
  }

  case class DummyBinds[E](fields: Set[Field] = Set.empty) extends Binds[E]

  implicit class RichString(queryText: String) {
    def model: QueryModel[Expr] = ir.model

    def ir: CypherQuery[Expr] = {
      val (stmt, _) = CypherParser.parseAndExtract(queryText)
      CypherQueryBuilder.plan(stmt)(IRBuilderContext(queryText, GlobalsExtractor(stmt), Schema.empty))
    }
  }
}
