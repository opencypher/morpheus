package org.opencypher.spark.prototype.impl.logical

import org.opencypher.spark.api.types.{CTAny, CTFloat, CTString}
import org.opencypher.spark.prototype.IrTestSuite
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.{ConstantRef, GlobalsRegistry, PropertyKeyRef}
import org.opencypher.spark.prototype.api.ir.pattern.{DirectedRelationship, EveryNode, EveryRelationship, Pattern}
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, ProjectedField}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.logical
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.language.implicitConversions

class LogicalPlannerTest extends IrTestSuite {

  test("convert match block") {
    val pattern = Pattern.empty[Expr]
      .withEntity('a, EveryNode)
      .withEntity('b, EveryNode)
      .withEntity('r, EveryRelationship)
      .withConnection('r, DirectedRelationship('a, 'b))

    val block = matchBlock(pattern)

    val scan = NodeScan('a, EveryNode)(emptySqm.withField('a))
    plan(irFor(block)) should equalWithoutResult(
      ExpandSource('a, 'r, EveryRelationship, 'b, scan)(scan.solved.withFields('r, 'b))
    )
  }

  val emptySqm = SolvedQueryModel.empty[Expr]

  test("convert project block") {
    val fields = ProjectedFields[Expr](Map(toField('a) -> Property('n, PropertyKeyRef(0))))
    val block = project(fields)

    plan(irWithLeaf(block)) should equalWithoutResult(
      Project(ProjectedField('a, Property('n, PropertyKeyRef(0)), CTAny.nullable),
        leafPlan)(emptySqm.withFields('n, 'a))
    )
  }

  test("plan query") {
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = 'Group-1' RETURN a.name".ir

    val globals = ir.model.globals

    plan(ir, globals) should equal(
      Select(Set(Var("a.name")),
        Project(ProjectedField(Var("a.name"), Property(Var("a"), globals.propertyKey("name")), CTAny.nullable),
          Filter(Equals(Property(Var("g"), globals.propertyKey("name")), Const(ConstantRef(0))),
            Project(ProjectedExpr(Property(Var("g"), globals.propertyKey("name")), CTAny.nullable),
              Filter(HasLabel(Var("g"), globals.label("Group")),
                Filter(HasLabel(Var("a"), globals.label("Administrator")),
                  ExpandSource(Var("a"), Var("r"), EveryRelationship, Var("g"),
                    NodeScan(Var("a"), EveryNode)(emptySqm)
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
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = 'Group-1' RETURN a.name".ir

    val globals = ir.model.globals
    val schema = Schema.empty
      .withNodeKeys("Group")("name" -> CTString)
      .withNodeKeys("Administrator")("name" -> CTFloat)

    plan(ir, globals, schema) should equal(
      Select(Set(Var("a.name")),
        Project(ProjectedField(Var("a.name"), Property(Var("a"), globals.propertyKey("name")), CTFloat),
          Filter(Equals(Property(Var("g"), globals.propertyKey("name")), Const(ConstantRef(0))),
            Project(ProjectedExpr(Property(Var("g"), globals.propertyKey("name")), CTString),
              Filter(HasLabel(Var("g"), globals.label("Group")),
                Filter(HasLabel(Var("a"), globals.label("Administrator")),
                  ExpandSource(Var("a"), Var("r"), EveryRelationship, Var("g"),
                    NodeScan(Var("a"), EveryNode)(emptySqm)
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
    val ir = "MATCH (a) WHERE NOT 1 = false RETURN a.prop".ir

    val globals = ir.model.globals

    plan(ir, globals) should equal(
      Select(Set(Var("a.prop")),
        Project(ProjectedField(Var("a.prop"), Property(Var("a"), globals.propertyKey("prop")), CTAny.nullable),
          Filter(Not(Equals(Const(globals.constant("  AUTOINT0")), Const(globals.constant("  AUTOBOOL1")))),
            NodeScan(Var("a"), EveryNode)(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  private val producer = new LogicalPlanner

  private def plan(ir: CypherQuery[Expr], globalsRegistry: GlobalsRegistry = GlobalsRegistry.none, schema: Schema = Schema.empty): LogicalOperator =
    producer.plan(ir)(LogicalPlannerContext(schema, globalsRegistry))

  case class equalWithoutResult(plan: LogicalOperator) extends Matcher[LogicalOperator] {
    override def apply(left: LogicalOperator): MatchResult = {
      left match {
        case logical.Select(_, in, _) =>
          val matches = in == plan && in.solved == plan.solved
          MatchResult(matches, s"$in did not equal $plan", s"$in was not supposed to equal $plan")
        case _ => MatchResult(matches = false, "Expected a Select plan on top", "Expected a Select plan on top")
      }
    }
  }
}
