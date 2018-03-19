/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.relational.impl.flat

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{IRField, Label, PropertyKey}
import org.opencypher.okapi.ir.test._
import org.opencypher.okapi.ir.test.support.MatchHelper._
import org.opencypher.okapi.logical.impl.{Directed, LogicalGraph, LogicalOperatorProducer, Undirected}
import org.opencypher.okapi.relational.impl.table.{FieldSlotContent, OpaqueField, ProjectedExpr, ProjectedField}
import org.opencypher.okapi.test.BaseTestSuite

import scala.language.implicitConversions

case class TestGraph(schema: Schema) extends LogicalGraph {
  override protected def args: String = ""
}

class FlatPlannerTest extends BaseTestSuite {

  val schema = Schema.empty
    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
    .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTFloat)
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTString)
    .withRelationshipPropertyKeys("FOO")("bar" -> CTBoolean)

  implicit val flatContext: FlatPlannerContext = FlatPlannerContext(CypherMap.empty)

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
    headerContents should equal(
      Set(
        ProjectedField('c, expr)
      ))
  }

  test("construct load graph") {
    flatPlanner.process(logicalStartOperator) should equal(flatStartOperator)
  }

  test("Construct node scan") {
    val result = flatPlanner.process(logicalNodeScan("n", "Person"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode("Person"))

    result should equal(flatNodeScan(nodeVar))
    headerContents should equal(
      Set(
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
    headerContents should equal(
      Set(
        OpaqueField(nodeVar),
        ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(nodeVar, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable))
      ))
  }

  test("Construct simple filtered node scan") {
    val result = flatPlanner.process(
      mkLogical.planFilter(TrueLit(), logicalNodeScan("n"))
    )
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(
      mkFlat.filter(
        TrueLit(),
        flatNodeScan(nodeVar)
      )
    )
    headerContents should equal(
      Set(
        OpaqueField(nodeVar),
        ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(nodeVar, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable))
      ))
  }

  test("flat plan for expand") {
    val result = flatPlanner.process(
      mkLogical.planExpand(
        IRField("n")(CTNode),
        IRField("r")(CTRelationship),
        IRField("m")(CTNode),
        Undirected,
        logicalNodeScan("n"),
        logicalNodeScan("m"))
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship)
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expand(source, rel, Undirected, target, schema, flatNodeScan(source), flatNodeScan(target))
    )
    headerContents should equal(
      Set(
        OpaqueField(source),
        ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat.nullable)),
        ProjectedExpr(StartNode(rel)(CTInteger)),
        OpaqueField(rel),
        ProjectedExpr(Type(rel)(CTString)),
        ProjectedExpr(EndNode(rel)(CTInteger)),
        ProjectedExpr(Property(rel, PropertyKey("since"))(CTString.nullable)),
        ProjectedExpr(Property(rel, PropertyKey("bar"))(CTBoolean.nullable)),
        OpaqueField(target),
        ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat.nullable))
      ))
  }

  test("flat plan for expand with rel type info") {
    val result = flatPlanner.process(
      mkLogical.planExpand(
        IRField("n")(CTNode),
        IRField("r")(CTRelationship("KNOWS")),
        IRField("m")(CTNode),
        Directed,
        logicalNodeScan("n"),
        logicalNodeScan("m")
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship("KNOWS"))
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expand(source, rel, Directed, target, schema, flatNodeScan(source), flatNodeScan(target))
    )
    headerContents should equal(
      Set(
        OpaqueField(source),
        ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat.nullable)),
        ProjectedExpr(StartNode(rel)(CTInteger)),
        OpaqueField(rel),
        ProjectedExpr(Type(rel)(CTString)),
        ProjectedExpr(EndNode(rel)(CTInteger)),
        ProjectedExpr(Property(rel, PropertyKey("since"))(CTString)),
        OpaqueField(target),
        ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat.nullable))
      ))
  }

  test("flat plan for init var expand") {
    val sourceScan = logicalNodeScan("n")
    val targetScan = logicalNodeScan("m")
    val logicalPlan = mkLogical.planBoundedVarLengthExpand(
      'n -> CTNode,
      'r -> CTList(CTRelationship),
      'm -> CTNode,
      Directed,
      1,
      1,
      sourceScan,
      targetScan)
    val result = flatPlanner.process(logicalPlan)

    val source = Var("n")(CTNode)
    val edgeList = Var("r")(CTList(CTRelationship))
    val target = Var("m")(CTNode)

    val initVarExpand = mkFlat.initVarExpand(source, edgeList, flatNodeScan(source))

    val edgeScan = flatVarLengthEdgeScan(initVarExpand.edgeList)
    val flatOp = mkFlat.boundedVarExpand(
      edgeScan.edge,
      edgeList,
      target,
      Directed,
      1,
      1,
      initVarExpand,
      edgeScan,
      flatNodeScan(target),
      isExpandInto = false)

    result should equal(flatOp)

    result.header.contents should equal(
      Set(
        OpaqueField(source),
        ProjectedExpr(HasLabel(source, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(source, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(source, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(source, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(source, PropertyKey("salary"))(CTFloat.nullable)),
        OpaqueField(edgeList),
        OpaqueField(target),
        ProjectedExpr(HasLabel(target, Label("Person"))(CTBoolean)),
        ProjectedExpr(HasLabel(target, Label("Employee"))(CTBoolean)),
        ProjectedExpr(Property(target, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(target, PropertyKey("age"))(CTInteger.nullable)),
        ProjectedExpr(Property(target, PropertyKey("salary"))(CTFloat.nullable))
      ))
  }

  ignore("Construct label-filtered node scan") {
    val nodeVar = Var("n")(CTNode)

    val result = flatPlanner.process(
      mkLogical.planFilter(HasLabel(nodeVar, Label("Person"))(CTBoolean), logicalNodeScan("n"))
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.filter(
        HasLabel(nodeVar, Label("Person"))(CTBoolean),
        flatNodeScan(nodeVar.name)
      )
    )
    headerContents should equal(
      Set(
        OpaqueField(nodeVar),
        ProjectedExpr(HasLabel(nodeVar, Label("Person"))(CTBoolean)),
        ProjectedExpr(Property(nodeVar, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(nodeVar, PropertyKey("age"))(CTInteger.nullable))
      ))
  }

  test("Construct selection") {
    val result = flatPlanner.process(
      mkLogical.planSelect(
        IndexedSeq(Var("foo")(CTString)),
        prev = mkLogical.projectField(
          IRField("foo")(CTString),
          Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person"))
      )
    )
    val headerContents = result.header.contents

    result should equalWithTracing(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString)),
        Set.empty,
        mkFlat.project(
          ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString)),
          flatNodeScan("n", "Person")
        )
      )
    )
    headerContents should equalWithTracing(
      Set(
        ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString))
      ))
  }

  test("Construct selection with renamed alias") {
    val n = Var("n")(CTNode("Person"))

    val result = flatPlanner.process(
      mkLogical.planSelect(
        IndexedSeq(n),
        prev = mkLogical.projectField(
          IRField("foo")(CTString),
          Property(n, PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person")))
    )
    val headerContents = result.header.contents

    result should equalWithTracing(
      mkFlat.select(
        IndexedSeq(n),
        Set.empty,
        mkFlat.removeAliases(
          IndexedSeq(n),
          mkFlat.project(
            ProjectedField(Var("foo")(CTString), Property(n, PropertyKey("name"))(CTString)),
            flatNodeScan("n", "Person")
          ))
      )
    )
    headerContents should equalWithTracing(
      Set(
        OpaqueField(n),
        ProjectedExpr(HasLabel(n, Label("Person"))(CTBoolean)),
        ProjectedExpr(Property(n, PropertyKey("name"))(CTString)),
        ProjectedExpr(Property(n, PropertyKey("age"))(CTInteger.nullable))
      ))

  }

  test("Construct selection with several fields") {
    val result = flatPlanner.process(
      mkLogical.planSelect(
        IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        prev = mkLogical.projectField(
          IRField("baz")(CTInteger),
          Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable),
          mkLogical.projectField(
            IRField("foo")(CTString),
            Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
            logicalNodeScan("n", "Person"))
        )
      )
    )
    val orderedContents = result.header.slots.map(_.content).collect { case content: FieldSlotContent => content.key }

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        Set.empty,
        mkFlat.project(
          ProjectedField(
            Var("baz")(CTInteger.nullable),
            Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable)),
          mkFlat.project(
            ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), PropertyKey("name"))(CTString)),
            flatNodeScan("n", "Person"))
        )
      )
    )
    orderedContents should equal(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger)))
  }

  private def logicalNodeScan(nodeField: String, labelNames: String*) =
    mkLogical.planNodeScan(IRField(nodeField)(CTNode(labelNames.toSet)), logicalStartOperator)

  private def flatNodeScan(node: Var): NodeScan =
    mkFlat.nodeScan(node, flatStartOperator)

  private def flatNodeScan(node: String, labelNames: String*): NodeScan =
    flatNodeScan(Var(node)(CTNode(labelNames.toSet)))

  private def flatVarLengthEdgeScan(edgeList: Var) =
    mkFlat.varLengthEdgeScan(edgeList, flatStartOperator)
}
