package org.opencypher.spark.prototype.impl.planner

import javax.print.DocFlavor.STRING

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.spark.prototype.api.expr.{HasLabel, Property, Var}
import org.opencypher.spark.prototype.api.ir.Field
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllOf, EveryNode}
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, RecordSlot}
import org.opencypher.spark.prototype.api.schema.Schema

class PhysicalPlannerTest extends StdTestSuite {

  val schema = Schema
    .empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)

  val globals = GlobalsRegistry.fromSchema(schema)

  implicit val context = PhysicalPlannerContext(schema, globals)

  import globals._

  val mkLogical = new LogicalOperatorProducer
  val physicalPlanner = new PhysicalPlanner

  test("Construct node scan") {
    val mkPhysical = new PhysicalOperatorProducer()

    val result = physicalPlanner.plan(mkLogical.planNodeScan(Field("n"), EveryNode(AllOf(label("Person")))))
    val slots = result.header.slots

    result should equal(mkPhysical.nodeScan(Var("n"), EveryNode(AllOf(label("Person")))))
    slots should equal(Seq(
      RecordSlot(0,ProjectedExpr(HasLabel(Var("n"), label("Person")), CTBoolean)),
      RecordSlot(1,ProjectedExpr(Property(Var("n"), propertyKey("name")), CTString)),
      RecordSlot(2,ProjectedExpr(Property(Var("n"), propertyKey("age")), CTInteger.nullable))
    ))
  }
}
