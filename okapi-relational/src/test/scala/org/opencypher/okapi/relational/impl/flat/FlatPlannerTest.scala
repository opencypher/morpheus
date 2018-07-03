///*
// * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Attribution Notice under the terms of the Apache License 2.0
// *
// * This work was created by the collective efforts of the openCypher community.
// * Without limiting the terms of Section 6, any Derivative Work that is not
// * approved by the public consensus process of the openCypher Implementers Group
// * should not be described as “Cypher” (and Cypher® is a registered trademark of
// * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
// * proposals for change that have been documented or implemented should only be
// * described as "implementation extensions to Cypher" or as "proposed changes to
// * Cypher that are not yet approved by the openCypher community".
// */
//package org.opencypher.okapi.relational.impl.flat
//
//import org.opencypher.okapi.api.schema.Schema
//import org.opencypher.okapi.api.types._
//import org.opencypher.okapi.api.value.CypherValue.CypherMap
//import org.opencypher.okapi.ir.api.expr._
//import org.opencypher.okapi.ir.api.{IRField, Label, PropertyKey, RelType}
//import org.opencypher.okapi.ir.impl.util.VarConverters._
//import org.opencypher.okapi.logical.impl.{Directed, LogicalGraph, LogicalOperatorProducer, Undirected}
//import org.opencypher.okapi.relational.impl.table.RecordHeader
//import org.opencypher.okapi.testing.BaseTestSuite
//import org.opencypher.okapi.testing.MatchHelper._
//
//import scala.language.implicitConversions
//
//case class TestGraph(schema: Schema) extends LogicalGraph {
//  override protected def args: String = ""
//}
//
//class FlatPlannerTest extends BaseTestSuite {
//
//  val schema = Schema.empty
//    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
//    .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTFloat)
//    .withRelationshipPropertyKeys("KNOWS")("since" -> CTString)
//    .withRelationshipPropertyKeys("FOO")("bar" -> CTBoolean)
//
//  // TODO: Re-enable header assertions once relational/physical planners are merged
//  implicit val flatContext: FlatPlannerContext = FlatPlannerContext(CypherMap.empty, RecordHeader.empty)
//
//  val mkLogical = new LogicalOperatorProducer
//  val mkFlat = new FlatOperatorProducer()
//  val flatPlanner = new FlatPlanner
//
//  val logicalStartOperator = mkLogical.planStart(TestGraph(schema), Set.empty)
//  val flatStartOperator = mkFlat.planStart(TestGraph(schema))
//
//  test("projecting a new expression") {
//    val expr = Subtract('a, 'b)()
//    val result = flatPlanner.process(mkLogical.projectField(expr, 'c, logicalStartOperator))
////    val headerExpressions = result.header.expressions
//
//    result should equalWithTracing(mkFlat.project(expr -> Some('c), flatStartOperator))
////    headerExpressions should equalWithTracing(Set(expr, Var("c")()))
//  }
//
//  test("construct load graph") {
//    flatPlanner.process(logicalStartOperator) should equalWithTracing(flatStartOperator)
//  }
//
//  test("Construct node scan") {
//    val result = flatPlanner.process(logicalNodeScan("n", "Person"))
////    val headerExpressions = result.header.expressions
//
//    val nodeVar = Var("n")(CTNode("Person"))
//
//    result should equalWithTracing(flatNodeScan(nodeVar))
////    headerExpressions should equalWithTracing(
////      Set(
////        nodeVar,
////        HasLabel(nodeVar, Label("Person"))(CTBoolean),
////        Property(nodeVar, PropertyKey("name"))(CTString),
////        Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)
////      ))
//  }
//
//  test("Construct unlabeled node scan") {
//    val result = flatPlanner.process(logicalNodeScan("n"))
////    val headerExpressions = result.header.expressions
//
//    val nodeVar = Var("n")(CTNode)
//
//    result should equalWithTracing(flatNodeScan(nodeVar))
////    headerExpressions should equalWithTracing(
////      Set(
////        nodeVar,
////        HasLabel(nodeVar, Label("Person"))(CTBoolean),
////        HasLabel(nodeVar, Label("Employee"))(CTBoolean),
////        Property(nodeVar, PropertyKey("name"))(CTString),
////        Property(nodeVar, PropertyKey("age"))(CTInteger.nullable),
////        Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable)
////      ))
//  }
//
//  test("Construct simple filtered node scan") {
//    val result = flatPlanner.process(
//      mkLogical.planFilter(TrueLit, logicalNodeScan("n"))
//    )
////    val headerExpressions = result.header.expressions
//
//    val nodeVar = Var("n")(CTNode)
//
//    result should equalWithTracing(
//      mkFlat.filter(
//        TrueLit,
//        flatNodeScan(nodeVar)
//      )
//    )
////    headerExpressions should equalWithTracing(
////      Set(
////        nodeVar,
////        HasLabel(nodeVar, Label("Person"))(CTBoolean),
////        HasLabel(nodeVar, Label("Employee"))(CTBoolean),
////        Property(nodeVar, PropertyKey("name"))(CTString),
////        Property(nodeVar, PropertyKey("age"))(CTInteger.nullable),
////        Property(nodeVar, PropertyKey("salary"))(CTFloat.nullable)
////      ))
//  }
//
//  test("flat plan for expand") {
//    val result = flatPlanner.process(
//      mkLogical.planExpand(
//        IRField("n")(CTNode),
//        IRField("r")(CTRelationship),
//        IRField("m")(CTNode),
//        Undirected,
//        logicalNodeScan("n"),
//        logicalNodeScan("m"))
//    )
////    val headerExpressions = result.header.expressions
//
//    val source = Var("n")(CTNode)
//    val rel = Var("r")(CTRelationship)
//    val target = Var("m")(CTNode)
//
//    result should equalWithTracing(
//      mkFlat.expand(source, rel, Undirected, target, schema, flatNodeScan(source), flatNodeScan(target))
//    )
////    headerExpressions should equalWithTracing(
////      Set(
////        source,
////        HasLabel(source, Label("Person"))(CTBoolean),
////        HasLabel(source, Label("Employee"))(CTBoolean),
////        Property(source, PropertyKey("name"))(CTString),
////        Property(source, PropertyKey("age"))(CTInteger.nullable),
////        Property(source, PropertyKey("salary"))(CTFloat.nullable),
////        StartNode(rel)(CTInteger),
////        rel,
////        HasType(rel, RelType("FOO"))(CTBoolean),
////        HasType(rel, RelType("KNOWS"))(CTBoolean),
////        EndNode(rel)(CTInteger),
////        Property(rel, PropertyKey("since"))(CTString.nullable),
////        Property(rel, PropertyKey("bar"))(CTBoolean.nullable),
////        target,
////        HasLabel(target, Label("Person"))(CTBoolean),
////        HasLabel(target, Label("Employee"))(CTBoolean),
////        Property(target, PropertyKey("name"))(CTString),
////        Property(target, PropertyKey("age"))(CTInteger.nullable),
////        Property(target, PropertyKey("salary"))(CTFloat.nullable)
////      )
////    )
//  }
//
//  test("flat plan for expand with rel type info") {
//    val result = flatPlanner.process(
//      mkLogical.planExpand(
//        IRField("n")(CTNode),
//        IRField("r")(CTRelationship("KNOWS")),
//        IRField("m")(CTNode),
//        Directed,
//        logicalNodeScan("n"),
//        logicalNodeScan("m")
//      )
//    )
////    val headerExpressions = result.header.expressions
//
//    val source = Var("n")(CTNode)
//    val rel = Var("r")(CTRelationship("KNOWS"))
//    val target = Var("m")(CTNode)
//
//    result should equalWithTracing(
//      mkFlat.expand(source, rel, Directed, target, schema, flatNodeScan(source), flatNodeScan(target))
//    )
////    headerExpressions should equalWithTracing(
////      Set(
////        source,
////        HasLabel(source, Label("Person"))(CTBoolean),
////        HasLabel(source, Label("Employee"))(CTBoolean),
////        Property(source, PropertyKey("name"))(CTString),
////        Property(source, PropertyKey("age"))(CTInteger.nullable),
////        Property(source, PropertyKey("salary"))(CTFloat.nullable),
////        StartNode(rel)(CTInteger),
////        rel,
////        HasType(rel, RelType("KNOWS"))(CTBoolean),
////        EndNode(rel)(CTInteger),
////        Property(rel, PropertyKey("since"))(CTString),
////        target,
////        HasLabel(target, Label("Person"))(CTBoolean),
////        HasLabel(target, Label("Employee"))(CTBoolean),
////        Property(target, PropertyKey("name"))(CTString),
////        Property(target, PropertyKey("age"))(CTInteger.nullable),
////        Property(target, PropertyKey("salary"))(CTFloat.nullable)
////      ))
//  }
//
////  test("flat plan for init var expand") {
////    val sourceScan = logicalNodeScan("n")
////    val targetScan = logicalNodeScan("m")
////    val logicalPlan = mkLogical.planBoundedVarLengthExpand(
////      'n -> CTNode,
////      'r -> CTList(CTRelationship),
////      'm -> CTNode,
////      Directed,
////      1,
////      1,
////      sourceScan,
////      targetScan)
////    val result = flatPlanner.process(logicalPlan)
////
////    val source = Var("n")(CTNode)
////    val edgeList = Var("r")(CTList(CTRelationship))
////    val target = Var("m")(CTNode)
////
////    val initVarExpand = mkFlat.initVarExpand(source, edgeList, flatNodeScan(source))
////
////    val edgeScan = flatVarLengthEdgeScan(initVarExpand.edgeList)
////    val flatOp = mkFlat.boundedVarExpand(
////      edgeScan.rel,
////      edgeList,
////      target,
////      Directed,
////      1,
////      1,
////      initVarExpand,
////      edgeScan,
////      flatNodeScan(target),
////      isExpandInto = false)
////
////    result should equalWithTracing(flatOp)
////
////    val headerExpressions = result.header.expressions should equalWithTracing(
////      Set(
////        source,
////        HasLabel(source, Label("Person"))(CTBoolean),
////        HasLabel(source, Label("Employee"))(CTBoolean),
////        Property(source, PropertyKey("name"))(CTString),
////        Property(source, PropertyKey("age"))(CTInteger.nullable),
////        Property(source, PropertyKey("salary"))(CTFloat.nullable),
////        edgeList,
////        target,
////        HasLabel(target, Label("Person"))(CTBoolean),
////        HasLabel(target, Label("Employee"))(CTBoolean),
////        Property(target, PropertyKey("name"))(CTString),
////        Property(target, PropertyKey("age"))(CTInteger.nullable),
////        Property(target, PropertyKey("salary"))(CTFloat.nullable)
////      ))
////  }
//
//  ignore("Construct label-filtered node scan") {
//    val nodeVar = Var("n")(CTNode)
//
//    val result = flatPlanner.process(
//      mkLogical.planFilter(HasLabel(nodeVar, Label("Person"))(CTBoolean), logicalNodeScan("n"))
//    )
////    val headerExpressions = result.header.expressions
//
//    result should equalWithTracing(
//      mkFlat.filter(
//        HasLabel(nodeVar, Label("Person"))(CTBoolean),
//        flatNodeScan(nodeVar.name)
//      )
//    )
////    headerExpressions should equalWithTracing(
////      Set(
////        nodeVar,
////        HasLabel(nodeVar, Label("Person"))(CTBoolean),
////        Property(nodeVar, PropertyKey("name"))(CTString),
////        Property(nodeVar, PropertyKey("age"))(CTInteger.nullable)
////      ))
//  }
//
//  it("Construct selection") {
//    val result = flatPlanner.process(
//      mkLogical.planSelect(
//        List(Var("foo")(CTString)),
//        prev = mkLogical.projectField(
//          Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
//          IRField("foo")(CTString),
//          logicalNodeScan("n", "Person"))
//      )
//    )
////    val headerExpressions = result.header.expressions
//
////    result should equalWithTracing(
////      mkFlat.select(
////        List(Var("foo")(CTString)),
////        mkFlat.project(
////          Property(Var("n")(CTNode), PropertyKey("name"))(CTString) -> Some(Var("foo")(CTString)),
////          flatNodeScan("n", "Person")
////        )
////      )
////    )
////    headerExpressions should equalWithTracing(Set(Var("foo")(CTString)))
//  }
//
//  test("Construct selection with several fields") {
//    val result = flatPlanner.process(
//      mkLogical.planSelect(
//        List(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
//        prev = mkLogical.projectField(
//          Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable),
//          IRField("baz")(CTInteger),
//          mkLogical.projectField(
//            Property(Var("n")(CTNode), PropertyKey("name"))(CTString),
//            IRField("foo")(CTString),
//            logicalNodeScan("n", "Person"))
//        )
//      )
//    )
////    val header = result.header
////    val headerExpressions = header.expressions
//
//    result should equalWithTracing(
//      mkFlat.select(
//        List(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
//        mkFlat.project(
//          Property(Var("n")(CTNode), PropertyKey("age"))(CTInteger.nullable) -> Some(Var("baz")(CTInteger.nullable)),
//          mkFlat.project(
//            Property(Var("n")(CTNode), PropertyKey("name"))(CTString) -> Some(Var("foo")(CTString)),
//            flatNodeScan("n", "Person"))
//        )
//      )
//    )
//
////    headerExpressions should equalWithTracing(
////      header.ownedBy(Var("n")(CTNode)) ++ Set(
////        Var("foo")(CTString),
////        Var("baz")(CTInteger)))
//  }
//
//  private def logicalNodeScan(nodeField: String, labelNames: String*) =
//    mkLogical.planNodeScan(IRField(nodeField)(CTNode(labelNames.toSet)), logicalStartOperator)
//
//  private def flatNodeScan(node: Var): NodeScan =
//    mkFlat.nodeScan(node, flatStartOperator)
//
//  private def flatNodeScan(node: String, labelNames: String*): NodeScan =
//    flatNodeScan(Var(node)(CTNode(labelNames.toSet)))
//}
