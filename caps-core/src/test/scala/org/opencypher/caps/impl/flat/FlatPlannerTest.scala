/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.flat

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, ProjectedField}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.logical.{LogicalGraph, LogicalOperatorProducer}
import org.opencypher.caps.ir.api.pattern._
import org.opencypher.caps.ir.api.{IRField, Label, PropertyKey, RelType}
import org.opencypher.caps.test.BaseTestSuite

import scala.language.implicitConversions

case class TestGraph(schema: Schema) extends LogicalGraph {
  override val name = "TestGraph"
}

class FlatPlannerTest extends BaseTestSuite {

  val schema = Schema
    .empty
    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
    .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTFloat)
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTString)
    .withRelationshipPropertyKeys("FOO")("bar" -> CTBoolean)

  schema.verify

  implicit val flatContext: FlatPlannerContext = FlatPlannerContext(Map.empty)

  val mkLogical = new LogicalOperatorProducer
  val mkFlat = new FlatOperatorProducer()
  val flatPlanner = new FlatPlanner

  val logicalStartOperator = mkLogical.planStart(TestGraph(schema), Set.empty)
  val flatStartOperator = mkFlat.planStart(TestGraph(schema), logicalStartOperator.fields)

  test("projecting a new expression") {
    val expr = Subtract('a, 'b)()
    val result = flatPlanner.process(mkLogical.projectField('c, expr, logicalStartOperator))
    val headerContents = result.header.contents

    result should equal(mkFlat.project(ProjectedField('c, expr), flatStartOperator))
    headerContents should equal(Set(
      ProjectedField('c, expr)
    ))
  }

  test("construct load graph") {
    flatPlanner.process(logicalStartOperator) should equal(flatStartOperator)
  }

  test("Construct node scan") {
    val result = flatPlanner.process(logicalNodeScan("n", "Person"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar, "Person"))
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable))
    ))
  }

  test("Construct unlabeled node scan") {
    val result = flatPlanner.process(logicalNodeScan("n"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar))
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, PropertyKey("salary"))(CTFloat))
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
      ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, PropertyKey("salary"))(CTFloat))
    ))
  }

  test("flat plan for expand") {
    val result = flatPlanner.process(
      mkLogical.planSourceExpand(IRField("n")(CTNode), IRField("r")(CTRelationship), EveryRelationship, IRField("m")(CTNode),
        logicalNodeScan("n"), logicalNodeScan("m")
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship)
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expandSource(source, rel, EveryRelationship, target, schema,
        flatNodeScan(source), flatNodeScan(target)
      )
    )
    headerContents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat)),
      ProjectedExpr(StartNode(rel)(CTInteger)),
      OpaqueField(rel),
      ProjectedExpr(OfType(rel)(CTString)),
      ProjectedExpr(EndNode(rel)(CTInteger)),
      ProjectedExpr(Property(rel, PropertyKey("since"))(CTString)),
      ProjectedExpr(Property(rel, PropertyKey("bar"))(CTBoolean)),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat))
    ))
  }

  test("flat plan for expand with rel type info") {
    val result = flatPlanner.process(
      mkLogical.planSourceExpand(
        IRField("n")(CTNode),
        IRField("r")(CTRelationship("KNOWS")), EveryRelationship(AnyOf(RelType("KNOWS"))),
        IRField("m")(CTNode),
        logicalNodeScan("n"), logicalNodeScan("m")
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship("KNOWS"))
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expandSource(source, rel, EveryRelationship(AnyOf(RelType("KNOWS"))), target, schema,
        flatNodeScan(source), flatNodeScan(target)
      )
    )
    headerContents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat)),
      ProjectedExpr(StartNode(rel)(CTInteger)),
      OpaqueField(rel),
      ProjectedExpr(OfType(rel)(CTString)),
      ProjectedExpr(EndNode(rel)(CTInteger)),
      ProjectedExpr(Property(rel, PropertyKey("since"))(CTString)),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat))
    ))
  }

  test("flat plan for init var expand") {
    val sourceScan = logicalNodeScan("n")
    val targetScan = logicalNodeScan("m")
    val logicalPlan = mkLogical.planBoundedVarLengthExpand('n -> CTNode, 'r -> CTList(CTRelationship), EveryRelationship, 'm -> CTNode, 1, 1, sourceScan, targetScan)
    val result = flatPlanner.process(logicalPlan)

    val source = Var("n")(CTNode)
    val edgeList = Var("r")(CTList(CTRelationship))
    val target = Var("m")(CTNode)

    val initVarExpand = mkFlat.initVarExpand(source, edgeList, flatNodeScan(source))

    val edgeScan = flatVarLengthEdgeScan(initVarExpand.edgeList)
    val flatOp = mkFlat.boundedVarExpand(edgeScan.edge, edgeList, target, 1, 1,
      initVarExpand, edgeScan, flatNodeScan(target), isExpandInto = false)

    result should equal(flatOp)

    result.header.contents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat)),
      OpaqueField(edgeList),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat))
    ))
  }

  ignore("Construct label-filtered node scan") {
    val nodeVar = Var("n")(CTNode)

    val result = flatPlanner.process(
      mkLogical.planFilter(HasLabel(nodeVar, Label("Person"))(CTBoolean),
        logicalNodeScan("n")
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.filter(
        HasLabel(nodeVar, Label("Person"))(CTBoolean),
        flatNodeScan(nodeVar)
      )
    )
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable))
    ))
  }

  test("Construct selection") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString)),
        prev = mkLogical.projectField(IRField("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person")
        )
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString)), Set.empty,
        mkFlat.project(
          ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString)),
          flatNodeScan(Var("n")(CTNode), "Person")
        )
      )
    )
    headerContents should equal(Set(
      ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString))
    ))
  }

  test("Construct selection with several fields") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        prev = mkLogical.projectField(IRField("baz")(CTInteger), Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable),
          mkLogical.projectField(IRField("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
            logicalNodeScan("n", "Person")
          )
        )
      )
    )
    val orderedContents = result.header.slots.map(_.content).collect { case content: FieldSlotContent => content.key }

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)), Set.empty,
        mkFlat.project(
          ProjectedField(Var("baz")(CTInteger.nullable), Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable)),
          mkFlat.project(ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString)),
            flatNodeScan(Var("n")(CTNode), "Person")
          )
        )
      )
    )
    orderedContents should equal(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger)))
  }

  private def logicalNodeScan(nodeField: String, labelNames: String*) = {
    val labels = labelNames.map(Label)

    mkLogical.planNodeScan(IRField(nodeField)(CTNode), EveryNode(AllOf(labels: _*)), logicalStartOperator)
  }

  private def flatNodeScan(node: Var, labelNames: String*) = {
    val labels = labelNames.map(Label)

    mkFlat.nodeScan(node, EveryNode(AllOf(labels: _*)), flatStartOperator)
  }

  private def flatVarLengthEdgeScan(edgeList: Var, edgeTypes: String*) = {
    val types = edgeTypes.map(RelType)

    mkFlat.varLengthEdgeScan(edgeList, EveryRelationship(AnyOf(types: _*)), flatStartOperator)
  }
}
