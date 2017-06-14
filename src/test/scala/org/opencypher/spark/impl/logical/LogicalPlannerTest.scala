package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.{ConstantRef, GlobalsRegistry, PropertyKeyRef}
import org.opencypher.spark.api.ir.pattern.{DirectedRelationship, EveryNode, EveryRelationship, Pattern}
import org.opencypher.spark.api.record.{ProjectedExpr, ProjectedField}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.ir.IrTestSuite
import org.opencypher.spark.impl.logical
import org.opencypher.spark.impl.util.toVar
import org.opencypher.spark.toField
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.language.implicitConversions

class LogicalPlannerTest extends IrTestSuite {

  val nodeA = Field("a")(CTNode)
  val nodeB = Field("b")(CTNode)
  val nodeG = Field("g")(CTNode)
  val relR = Field("r")(CTRelationship)

  test("convert load graph block") {
    plan(irFor(leafBlock)) should equal(Select(IndexedSeq.empty, leafPlan)(emptySqm))
  }

  test("convert match block") {
    val pattern = Pattern.empty[Expr]
      .withEntity(nodeA, EveryNode)
      .withEntity(nodeB, EveryNode)
      .withEntity(relR, EveryRelationship)
      .withConnection(relR, DirectedRelationship(nodeA, nodeB))

    val block = matchBlock(pattern)

    val scan1 = NodeScan(nodeA, EveryNode, leafPlan)(emptySqm.withField(nodeA))
    val scan2 = NodeScan(nodeB, EveryNode, leafPlan)(emptySqm.withField(nodeB))
    plan(irWithLeaf(block)) should equalWithoutResult(
      ExpandSource(nodeA, relR, EveryRelationship, nodeB, scan1, scan2)(emptySqm.withFields(nodeA, nodeB, relR))
    )
  }

  val emptySqm = SolvedQueryModel.empty[Expr]

  test("convert project block") {
    val fields = ProjectedFields[Expr](Map(toField('a) -> Property('n, PropertyKeyRef(0))(CTFloat)))
    val block = project(fields)

    plan(irWithLeaf(block)) should equalWithoutResult(
      Project(ProjectedField('a, Property('n, PropertyKeyRef(0))(CTFloat)),   // n is a dangling reference here
        leafPlan)(emptySqm.withFields('a))
    )
  }

  test("plan query") {
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams("foo" -> CTString)

    val globals = ir.model.globals

    plan(ir, globals) should equal(
      Select(IndexedSeq(Var("a.name")(CTVoid)),
        Project(ProjectedField(Var("a.name")(CTVoid), Property(Var("a")(CTNode("Administrator")), globals.propertyKeyRefByName("name"))(CTVoid)),
          Filter(Equals(Property(Var("g")(CTNode("Group")), globals.propertyKeyRefByName("name"))(CTVoid), Const(ConstantRef(0))(CTString))(CTBoolean),
            Project(ProjectedExpr(Property(Var("g")(CTNode("Group")), globals.propertyKeyRefByName("name"))(CTVoid)),
              Filter(HasLabel(Var("g")(CTNode), globals.labelRefByName("Group"))(CTBoolean),
                Filter(HasLabel(Var("a")(CTNode), globals.labelRefByName("Administrator"))(CTBoolean),
                  ExpandSource(Var("a")(CTNode), Var("r")(CTRelationship), EveryRelationship, Var("g")(CTNode),
                    NodeScan(Var("a")(CTNode), EveryNode,
                      LoadGraph(NamedLogicalGraph("default", Schema.empty), DefaultGraphSource)(emptySqm)
                    )(emptySqm),
                    NodeScan(Var("g")(CTNode), EveryNode,
                      LoadGraph(NamedLogicalGraph("default", Schema.empty), DefaultGraphSource)(emptySqm)
                    )(emptySqm)
                  )(emptySqm)
                )(emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  test("plan query with type information") {
    implicit val schema = Schema.empty
      .withNodeKeys("Group")("name" -> CTString)
      .withNodeKeys("Administrator")("name" -> CTFloat)

    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams("foo" -> CTString)

    val globals = ir.model.globals

    plan(ir, globals, schema) should equal(
      Select(IndexedSeq(Var("a.name")(CTFloat)),
        Project(ProjectedField(Var("a.name")(CTFloat), Property(Var("a")(CTNode("Administrator")), globals.propertyKeyRefByName("name"))(CTFloat)),
          Filter(Equals(Property(Var("g")(CTNode("Group")), globals.propertyKeyRefByName("name"))(CTString), Const(ConstantRef(0))(CTString))(CTBoolean),
            Project(ProjectedExpr(Property(Var("g")(CTNode("Group")), globals.propertyKeyRefByName("name"))(CTString)),
              Filter(HasLabel(Var("g")(CTNode), globals.labelRefByName("Group"))(CTBoolean),
                Filter(HasLabel(Var("a")(CTNode), globals.labelRefByName("Administrator"))(CTBoolean),
                  ExpandSource(Var("a")(CTNode), Var("r")(CTRelationship), EveryRelationship, Var("g")(CTNode),
                    NodeScan(Var("a")(CTNode), EveryNode,
                      LoadGraph(NamedLogicalGraph("default", schema), DefaultGraphSource)(emptySqm)
                    )(emptySqm),
                    NodeScan(Var("g")(CTNode), EveryNode,
                      LoadGraph(NamedLogicalGraph("default", schema), DefaultGraphSource)(emptySqm)
                    )(emptySqm)
                  )(emptySqm)
                )(emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  test("plan query with negation") {
    val ir = "MATCH (a) WHERE NOT $p1 = $p2 RETURN a.prop".irWithParams("p1" -> CTInteger, "p2" -> CTBoolean)

    val globals = ir.model.globals

    plan(ir, globals) should equal(
      Select(IndexedSeq(Var("a.prop")(CTVoid)),
        Project(ProjectedField(Var("a.prop")(CTVoid), Property(nodeA, globals.propertyKeyRefByName("prop"))(CTVoid)),
          Filter(Not(Equals(Const(globals.constantRefByName("p1"))(CTInteger), Const(globals.constantRefByName("p2"))(CTBoolean))(CTBoolean))(CTBoolean),
            NodeScan(nodeA, EveryNode,
              LoadGraph(NamedLogicalGraph("default", Schema.empty), DefaultGraphSource)(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  private val producer = new LogicalPlanner

  private def plan(ir: CypherQuery[Expr], globalsRegistry: GlobalsRegistry = GlobalsRegistry.none, schema: Schema = Schema.empty): LogicalOperator =
    producer.process(ir)(LogicalPlannerContext(schema, globalsRegistry))

  case class equalWithoutResult(plan: LogicalOperator) extends Matcher[LogicalOperator] {
    override def apply(left: LogicalOperator): MatchResult = {
      left match {
        case logical.Select(_, in) =>
          val matches = in == plan && in.solved == plan.solved
          MatchResult(matches, s"$in did not equal $plan", s"$in was not supposed to equal $plan")
        case _ => MatchResult(matches = false, "Expected a Select plan on top", "Expected a Select plan on top")
      }
    }
  }
}
