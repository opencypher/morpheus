package org.opencypher.spark.impl.flat

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.expr.{HasLabel, Property, TrueLit, Var}
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern.{AllOf, EveryNode}
import org.opencypher.spark.api.record.{OpaqueField, ProjectedExpr, ProjectedField, RecordSlot}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.impl.logical.LogicalOperatorProducer

class FlatPlannerTest extends StdTestSuite {

  val schema = Schema
    .empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
    .withNodeKeys("Employee")("name" -> CTString, "salary" -> CTFloat)

  val globals = GlobalsRegistry.fromSchema(schema)

  implicit val context = FlatPlannerContext(schema, globals)

  import globals._

  val mkLogical = new LogicalOperatorProducer
  val mkPhysical = new FlatOperatorProducer()
  val physicalPlanner = new FlatPlanner

  // TODO: Ids missing
  // TODO: Do not name schema provided columns

  test("Construct node scan") {
    val result = physicalPlanner.process(mkLogical.planNodeScan(Field("n")(CTNode), EveryNode(AllOf(label("Person")))))
    val slots = result.header.slots

    result should equal(mkPhysical.nodeScan(Var("n")(CTNode), EveryNode(AllOf(label("Person")))))
    slots should equal(Seq(
      RecordSlot(0, OpaqueField(Var("n")(CTNode))),
      RecordSlot(1, ProjectedExpr(HasLabel(Var("n")(CTNode), label("Person"))(CTBoolean))),
      RecordSlot(2, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("name"))(CTString))),
      RecordSlot(3, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("age"))(CTInteger.nullable)))
    ))
  }

  test("Construct unlabeled node scan") {
    val result = physicalPlanner.process(mkLogical.planNodeScan(Field("n")(CTNode), EveryNode))
    val slots = result.header.slots

    result should equal(mkPhysical.nodeScan(Var("n")(CTNode), EveryNode))
    slots should equal(Seq(
      RecordSlot(0, OpaqueField(Var("n")(CTNode))),
      RecordSlot(1, ProjectedExpr(HasLabel(Var("n")(CTNode), label("Person"))(CTBoolean))),
      RecordSlot(2, ProjectedExpr(HasLabel(Var("n")(CTNode), label("Employee"))(CTBoolean))),
      RecordSlot(3, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("name"))(CTString))),
      RecordSlot(4, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("age"))(CTInteger.nullable))),
      RecordSlot(5, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("salary"))(CTFloat.nullable)))
    ))
  }

  test("Construct filtered node scan") {
    val result = physicalPlanner.process(
      mkLogical.planFilter(TrueLit(),
        mkLogical.planNodeScan(Field("n")(CTNode), EveryNode)
      )
    )
    val slots = result.header.slots

    result should equal(
      mkPhysical.filter(
        TrueLit(),
        mkPhysical.nodeScan(Var("n")(CTNode), EveryNode)
      )
    )
    slots should equal(Seq(
      RecordSlot(0, OpaqueField(Var("n")(CTNode))),
      RecordSlot(1, ProjectedExpr(HasLabel(Var("n")(CTNode), label("Person"))(CTBoolean))),
      RecordSlot(2, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("name"))(CTString))),
      RecordSlot(3, ProjectedExpr(Property(Var("n")(CTNode), propertyKey("age"))(CTInteger.nullable)))
    ))
  }

  test("Construct selection") {
    val result = physicalPlanner.process(
      mkLogical.planSelect(Set(Var("foo")(CTString)),
        mkLogical.projectField(Field("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString),
          mkLogical.planNodeScan(Field("n")(CTNode), EveryNode(AllOf(label("Person"))))
        )
      )
    )
    val slots = result.header.slots

    result should equal(
      mkPhysical.select(
        Set(Var("foo")(CTString)),
        mkPhysical.project(
          ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString)),
          mkPhysical.nodeScan(
            Var("n")(CTNode), EveryNode(AllOf(label("Person")))
          )
        )
      )
    )
    slots should equal(Seq(
      RecordSlot(0, ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString)))
    ))
  }
}
