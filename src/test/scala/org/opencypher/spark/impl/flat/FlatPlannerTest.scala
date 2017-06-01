package org.opencypher.spark.impl.flat

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.block.DefaultGraph
import org.opencypher.spark.api.ir.global.{GlobalsRegistry, LabelRef}
import org.opencypher.spark.api.ir.pattern.{AllOf, EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.{OpaqueField, ProjectedExpr, ProjectedField}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
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
  val mkFlat = new FlatOperatorProducer()
  val flatPlanner = new FlatPlanner

  val logicalLoadGraph = mkLogical.planLoadDefaultGraph(schema)
  val flatLoadGraph = mkFlat.planLoadGraph(logicalLoadGraph.outGraph, logicalLoadGraph.source)

  // TODO: Ids missing
  // TODO: Do not name schema provided columns

  test("construct load graph") {
    flatPlanner.process(logicalLoadGraph) should equal(flatLoadGraph)
  }

  test("Construct node scan") {
    val result = flatPlanner.process(logicalNodeScan("n", "Person"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar, "Person"))
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, label("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKey("age"))(CTInteger.nullable))
    ))
  }

  test("Construct unlabeled node scan") {
    val result = flatPlanner.process(logicalNodeScan("n"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar))
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, propertyKey("salary"))(CTFloat))
    ))
  }

  test("Construct simple filtered node scan") {
    val result = flatPlanner.process(
      mkLogical.planFilter(TrueLit(),
        logicalNodeScan("n")
      )
    )
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(
      mkFlat.filter(
        TrueLit(),
        flatNodeScan(nodeVar)
      )
    )
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, propertyKey("salary"))(CTFloat))
    ))
  }

  test("flat plan for expand") {
    val result = flatPlanner.process(
      mkLogical.planSourceExpand(Field("n")(CTNode), Field("r")(CTRelationship) -> EveryRelationship, Field("m")(CTNode),
        logicalNodeScan("n"), logicalNodeScan("m")
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship)
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expandSource(source, rel, EveryRelationship, target,
        flatNodeScan(source), flatNodeScan(target)
      )
    )
    headerContents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(source, propertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, propertyKey("salary"))(CTFloat)),
      ProjectedExpr(StartNode(rel)(CTInteger)),
      OpaqueField(rel),
      ProjectedExpr(TypeId(rel)(CTInteger)),
      ProjectedExpr(EndNode(rel)(CTInteger)),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(target, propertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, propertyKey("salary"))(CTFloat))
    ))
  }

  ignore("Construct label-filtered node scan") {
    val nodeVar = Var("n")(CTNode)

    val result = flatPlanner.process(
      mkLogical.planFilter(HasLabel(nodeVar, label("Person"))(CTBoolean),
        logicalNodeScan("n")
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.filter(
        HasLabel(nodeVar, label("Person"))(CTBoolean),
        flatNodeScan(nodeVar)
      )
    )
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, label("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKey("age"))(CTInteger.nullable))
    ))
  }

  test("Construct selection") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString)),
        mkLogical.projectField(Field("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString),
          logicalNodeScan("n", "Person")
        )
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString)),
        mkFlat.project(
          ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString)),
          flatNodeScan(Var("n")(CTNode), "Person")
        )
      )
    )
    headerContents should equal(Set(
      ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString))
    ))
  }

  test("Construct selection with several fields") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        mkLogical.projectField(Field("baz")(CTInteger), Property(Var("n")(CTNode), propertyKey("age"))(CTInteger.nullable),
          mkLogical.projectField(Field("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString),
            logicalNodeScan("n", "Person")
          )
        )
      )
    )
    val orderedContents = result.header.slots.map(_.content.key)

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        mkFlat.project(
          ProjectedField(Var("baz")(CTInteger.nullable), Property(Var("n")(CTNode), propertyKey("age"))(CTInteger.nullable)),
          mkFlat.project(ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKey("name"))(CTString)),
            flatNodeScan(Var("n")(CTNode), "Person")
          )
        )
      )
    )
    orderedContents should equal(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger)))
  }

  private def logicalNodeScan(nodeField: String, labelNames: String*) = {
    val labelRefs = labelNames.map(label)

    mkLogical.planNodeScan(Field(nodeField)(CTNode), EveryNode(AllOf(labelRefs: _*)), logicalLoadGraph)
  }

  private def flatNodeScan(node: Var, labelNames: String*) = {
    val labelRefs = labelNames.map(label)

    mkFlat.nodeScan(node, EveryNode(AllOf(labelRefs: _*)), flatLoadGraph)
  }
}
