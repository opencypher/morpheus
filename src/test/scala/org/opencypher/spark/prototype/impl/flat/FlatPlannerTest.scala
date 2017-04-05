package org.opencypher.spark.prototype.impl.flat

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTNode, CTString}
import org.opencypher.spark.prototype.api.expr.{HasLabel, Property, TrueLit, Var}
import org.opencypher.spark.prototype.api.ir.Field
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllOf, EveryNode}
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, RecordSlot}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.logical.LogicalOperatorProducer

class FlatPlannerTest extends StdTestSuite {

  val schema = Schema
    .empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)

  val globals = GlobalsRegistry.fromSchema(schema)

  implicit val context = FlatPlannerContext(schema, globals)

  import globals._

  val mkLogical = new LogicalOperatorProducer
  val physicalPlanner = new FlatPlanner

  // TODO: Ids missing
  // TODO: Do not name schema provided columns

  test("Construct node scan") {
    val mkPhysical = new FlatOperatorProducer()

    val result = physicalPlanner.plan(mkLogical.planNodeScan(Field("n")(CTNode), EveryNode(AllOf(label("Person")))))
    val slots = result.header.slots

    result should equal(mkPhysical.nodeScan(Var("n"), EveryNode(AllOf(label("Person")))))
    slots should equal(Seq(
      RecordSlot(0, ProjectedExpr(HasLabel(Var("n"), label("Person")), CTBoolean)),
      RecordSlot(1, ProjectedExpr(Property(Var("n"), propertyKey("name")), CTString)),
      RecordSlot(2, ProjectedExpr(Property(Var("n"), propertyKey("age")), CTInteger.nullable))
    ))
  }

  test("Construct filtered node scan") {
    val mkPhysical = new FlatOperatorProducer()

    val result = physicalPlanner.plan(
      mkLogical.planFilter(TrueLit(),
        mkLogical.planNodeScan(Field("n")(CTNode), EveryNode(AllOf(label("Person"))))
      )
    )
    val slots = result.header.slots

    result should equal(mkPhysical.filter(
      TrueLit(),
      mkPhysical.nodeScan(Var("n"), EveryNode(AllOf(label("Person"))))
    ))
    slots should equal(Seq(
      RecordSlot(0, ProjectedExpr(HasLabel(Var("n"), label("Person")), CTBoolean)),
      RecordSlot(1, ProjectedExpr(Property(Var("n"), propertyKey("name")), CTString)),
      RecordSlot(2, ProjectedExpr(Property(Var("n"), propertyKey("age")), CTInteger.nullable))
    ))
  }
}
