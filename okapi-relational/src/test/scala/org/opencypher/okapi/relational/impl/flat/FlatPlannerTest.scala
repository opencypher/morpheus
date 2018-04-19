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
import org.opencypher.okapi.relational.impl.table.RecordHeader._
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

    result should equal(mkFlat.project(expr, Some('c), flatStartOperator))
    result.header should equal(Map(expr -> 'c))
  }

  test("construct load graph") {
    flatPlanner.process(logicalStartOperator) should equal(flatStartOperator)
  }

  test("Construct node scan") {
    val result = flatPlanner.process(logicalNodeScan("n", "Person"))

    val nodeVar = Var("n")(CTNode("Person"))

    result should equal(flatNodeScan(nodeVar))
    result.header should equal(Map(
      nodeVar -> Set(nodeVar),
      HasLabel(nodeVar, Label("Person"))(CTBoolean) -> Set.empty,
      Property(nodeVar, PropertyKey("name"))(CTString) -> Set.empty,
      Property(nodeVar, PropertyKey("age"))(CTInteger.nullable) -> Set.empty
    ))
  }

  test("Construct unlabeled node scan") {
    val result = flatPlanner.process(logicalNodeScan("n"))

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar))
    result.header should equal(Map(
      nodeVar -> Set(nodeVar),
      HasLabel(nodeVar, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(nodeVar, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(nodeVar, PropertyKey("name"))(CTString) -> Set.empty,
      Property(nodeVar, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty
    ))
  }

  test("Construct simple filtered node scan") {
    val result = flatPlanner.process(
      mkLogical.planFilter(TrueLit(), logicalNodeScan("n"))
    )
    val nodeVar = Var("n")(CTNode)

    result should equal(
      mkFlat.filter(
        TrueLit(),
        flatNodeScan(nodeVar)
      )
    )
    result.header should equal(Map(
      nodeVar -> Set(nodeVar),
      HasLabel(nodeVar, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(nodeVar, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(nodeVar, PropertyKey("name"))(CTString) -> Set.empty,
      Property(nodeVar, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty
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
    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship)
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expand(source, rel, Undirected, target, schema, flatNodeScan(source), flatNodeScan(target))
    )
    result.header should equal(Map(
      source -> Set(source),
      HasLabel(source, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(source, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(source, PropertyKey("name"))(CTString) -> Set.empty,
      Property(source, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(source, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty,
      StartNode(rel)(CTInteger) -> Set.empty,
      rel -> Set(rel),
      Type(rel)(CTString) -> Set.empty,
      EndNode(rel)(CTInteger) -> Set.empty,
      Property(rel, PropertyKey("since"))(CTString.nullable) -> Set.empty,
      Property(rel, PropertyKey("bar"))(CTBoolean.nullable) -> Set.empty,
      target -> Set(target),
      HasLabel(target, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(target, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(target, PropertyKey("name"))(CTString) -> Set.empty,
      Property(target, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(target, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty
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
    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship("KNOWS"))
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expand(source, rel, Directed, target, schema, flatNodeScan(source), flatNodeScan(target))
    )
    result.header should equal(Map(
      source -> Set(source),
      HasLabel(source, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(source, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(source, PropertyKey("name"))(CTString) -> Set.empty,
      Property(source, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(source, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty,
      StartNode(rel)(CTInteger) -> Set.empty,
      rel -> Set(rel),
      Type(rel)(CTString) -> Set.empty,
      EndNode(rel)(CTInteger) -> Set.empty,
      Property(rel, PropertyKey("since"))(CTString) -> Set.empty,
      target -> Set(target),
      HasLabel(target, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(target, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(target, PropertyKey("name"))(CTString) -> Set.empty,
      Property(target, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(target, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty
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

    result.header should equal(Map(
      source -> Set(source),
      HasLabel(source, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(source, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(source, PropertyKey("name"))(CTString) -> Set.empty,
      Property(source, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(source, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty,
      edgeList -> Set(edgeList),
      target -> Set(target),
      HasLabel(target, Label("Person"))(CTBoolean) -> Set.empty,
      HasLabel(target, Label("Employee"))(CTBoolean) -> Set.empty,
      Property(target, PropertyKey("name"))(CTString) -> Set.empty,
      Property(target, PropertyKey("age"))(CTInteger.nullable) -> Set.empty,
      Property(target, PropertyKey("salary"))(CTFloat.nullable) -> Set.empty
    ))
  }

  ignore("Construct label-filtered node scan") {
    val nodeVar = Var("n")(CTNode)

    val result = flatPlanner.process(
      mkLogical.planFilter(HasLabel(nodeVar, Label("Person"))(CTBoolean), logicalNodeScan("n"))
    )
    result should equal(
      mkFlat.filter(
        HasLabel(nodeVar, Label("Person"))(CTBoolean),
        flatNodeScan(nodeVar.name)
      )
    )
    result.header should equal(Map(
      nodeVar -> Set(nodeVar),
      HasLabel(nodeVar, Label("Person"))(CTBoolean) -> Set.empty,
      Property(nodeVar, PropertyKey("name"))(CTString) -> Set.empty,
      Property(nodeVar, PropertyKey("age"))(CTInteger.nullable) -> Set.empty
    ))
  }

  test("Construct selection") {
    val result = flatPlanner.process(
      mkLogical.planSelect(
        List(Var("foo")(CTString)),
        prev = mkLogical.projectField(
          IRField("foo")(CTString),
          Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person"))
      )
    )
    result should equalWithTracing(
      mkFlat.select(
        List(Var("foo")(CTString)),
        mkFlat.project(
          Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
          Some(Var("foo")(CTString)),
          flatNodeScan("n", "Person"))
      )
    )
    result.header should equalWithTracing(Map(
      Property(Var("n")(CTNode), PropertyKey("name"))(CTString) -> Set(Var("foo")(CTString))
    ))
  }

  test("Construct selection with renamed alias") {
    val n = Var("n")(CTNode("Person"))

    val header = flatPlanner.process(
      mkLogical.planSelect(
        List(n),
        prev = mkLogical.projectField(
          IRField("foo")(CTString),
          Property(n, PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person")))
    ).header

    flatPlanner.process(
      mkLogical.planSelect(
        List(n),
        prev = mkLogical.projectField(
          IRField("foo")(CTString),
          Property(n, PropertyKey("name"))(CTString),
          logicalNodeScan("n", "Person")))
    ) should equalWithTracing(
      mkFlat.select(
        List(n),
        mkFlat.select(
          List(n),
          mkFlat.project(
            Property(n, PropertyKey("name"))(CTString),
            Some(Var("foo")(CTString)),
            flatNodeScan("n", "Person")
          ))
      )
    )
    header should equalWithTracing(Map(
      n -> Set(n),
      HasLabel(n, Label("Person"))(CTBoolean) -> Set.empty,
      Property(n, PropertyKey("name"))(CTString) -> Set.empty,
      Property(n, PropertyKey("age"))(CTInteger.nullable) -> Set.empty
    ))
  }

  test("Construct selection with several fields") {
    val result = flatPlanner.process(
      mkLogical.planSelect(
        List(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
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

    result should equal(
      mkFlat.select(
        List(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        mkFlat.project(
          Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable),
          Some(Var("baz")(CTInteger.nullable)),
          mkFlat.project(
            Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
            Some(Var("foo")(CTString)),
            flatNodeScan("n", "Person"))
        )
      )
    )
    result.header should equal(Map(
      Var("baz")(CTInteger) -> Set(Var("baz")(CTInteger)),
      Var("foo")(CTString) -> Set(Var("foo")(CTString)),
      Var("n")(CTNode) -> Set(Var("n")(CTNode))
    ))
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
