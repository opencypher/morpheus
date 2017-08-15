/**
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
package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.global.{GlobalsRegistry, Label, RelType}
import org.opencypher.spark.api.ir.pattern._
import org.opencypher.spark.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, ProjectedField}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.logical.LogicalOperatorProducer
import org.opencypher.spark.{BaseTestSuite, toField}

class FlatPlannerTest extends BaseTestSuite {

  val schema = Schema
    .empty
    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
    .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTFloat)
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTString)
    .withRelationshipPropertyKeys("FOO")("bar" -> CTBoolean)

  schema.verify

  val globals = GlobalsRegistry.fromSchema(schema)

  implicit val context = FlatPlannerContext(schema, globals.tokens, globals.constants)

  import globals.tokens._
  import globals.constants._

  val mkLogical = new LogicalOperatorProducer
  val mkFlat = new FlatOperatorProducer()
  val flatPlanner = new FlatPlanner

  val logicalStartOperator = mkLogical.planStart(schema, Set.empty)
  val flatStartOperator = mkFlat.planStart(logicalStartOperator.outGraph, logicalStartOperator.source, logicalStartOperator.fields)

  // TODO: Ids missing
  // TODO: Do not name schema provided columns

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
      ProjectedExpr(HasLabel(nodeVar, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("age"))(CTInteger.nullable))
    ))
  }

  test("Construct unlabeled node scan") {
    val result = flatPlanner.process(logicalNodeScan("n"))
    val headerContents = result.header.contents

    val nodeVar = Var("n")(CTNode)

    result should equal(flatNodeScan(nodeVar))
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("salary"))(CTFloat))
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
      ProjectedExpr(HasLabel(nodeVar, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(nodeVar, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("salary"))(CTFloat))
    ))
  }

  test("flat plan for expand") {
    val result = flatPlanner.process(
      mkLogical.planSourceExpand(Field("n")(CTNode), Field("r")(CTRelationship), EveryRelationship, Field("m")(CTNode),
        logicalNodeScan("n"), logicalNodeScan("m"), false
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship)
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expandSource(source, rel, EveryRelationship, target,
        flatNodeScan(source), flatNodeScan(target), false
      )
    )
    headerContents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(source, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, propertyKeyByName("salary"))(CTFloat)),
      ProjectedExpr(StartNode(rel)(CTInteger)),
      OpaqueField(rel),
      ProjectedExpr(TypeId(rel)(CTInteger)),
      ProjectedExpr(EndNode(rel)(CTInteger)),
      ProjectedExpr(Property(rel, propertyKeyByName("since"))(CTString)),
      ProjectedExpr(Property(rel, propertyKeyByName("bar"))(CTBoolean)),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(target, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, propertyKeyByName("salary"))(CTFloat))
    ))
  }

  test("flat plan for expand with rel type info") {
    val result = flatPlanner.process(
      mkLogical.planSourceExpand(
        Field("n")(CTNode),
        Field("r")(CTRelationship("KNOWS")), EveryRelationship(AnyOf(RelType("KNOWS"))),
        Field("m")(CTNode),
        logicalNodeScan("n"), logicalNodeScan("m"),
        false
      )
    )
    val headerContents = result.header.contents

    val source = Var("n")(CTNode)
    val rel = Var("r")(CTRelationship("KNOWS"))
    val target = Var("m")(CTNode)

    result should equal(
      mkFlat.expandSource(source, rel, EveryRelationship(AnyOf(RelType("KNOWS"))), target,
        flatNodeScan(source), flatNodeScan(target), false
      )
    )
    headerContents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(source, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, propertyKeyByName("salary"))(CTFloat)),
      ProjectedExpr(StartNode(rel)(CTInteger)),
      OpaqueField(rel),
      ProjectedExpr(TypeId(rel)(CTInteger)),
      ProjectedExpr(EndNode(rel)(CTInteger)),
      ProjectedExpr(Property(rel, propertyKeyByName("since"))(CTString)),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(target, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, propertyKeyByName("salary"))(CTFloat))
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
      initVarExpand, edgeScan, flatNodeScan(target))

    result should equal(flatOp)

    result.header.contents should equal(Set(
      OpaqueField(source),
      ProjectedExpr(HasLabel(source, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(source, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(source, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(source, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(source, propertyKeyByName("salary"))(CTFloat)),
      OpaqueField(edgeList),
      OpaqueField(target),
      ProjectedExpr(HasLabel(target, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(HasLabel(target, labelByName("Employee"))(CTBoolean)),
      ProjectedExpr(Property(target, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(target, propertyKeyByName("age"))(CTInteger.nullable)),
      ProjectedExpr(Property(target, propertyKeyByName("salary"))(CTFloat))
    ))
  }

  ignore("Construct label-filtered node scan") {
    val nodeVar = Var("n")(CTNode)

    val result = flatPlanner.process(
      mkLogical.planFilter(HasLabel(nodeVar, labelByName("Person"))(CTBoolean),
        logicalNodeScan("n")
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.filter(
        HasLabel(nodeVar, labelByName("Person"))(CTBoolean),
        flatNodeScan(nodeVar)
      )
    )
    headerContents should equal(Set(
      OpaqueField(nodeVar),
      ProjectedExpr(HasLabel(nodeVar, labelByName("Person"))(CTBoolean)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("name"))(CTString)),
      ProjectedExpr(Property(nodeVar, propertyKeyByName("age"))(CTInteger.nullable))
    ))
  }

  test("Construct selection") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString)),
        mkLogical.projectField(Field("foo")(CTString), Property(Var("n")(CTNode), propertyKeyByName("name"))(CTString),
          logicalNodeScan("n", "Person")
        )
      )
    )
    val headerContents = result.header.contents

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString)),
        mkFlat.project(
          ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKeyByName("name"))(CTString)),
          flatNodeScan(Var("n")(CTNode), "Person")
        )
      )
    )
    headerContents should equal(Set(
      ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKeyByName("name"))(CTString))
    ))
  }

  test("Construct selection with several fields") {
    val result = flatPlanner.process(
      mkLogical.planSelect(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        mkLogical.projectField(Field("baz")(CTInteger), Property(Var("n")(CTNode), propertyKeyByName("age"))(CTInteger.nullable),
          mkLogical.projectField(Field("foo")(CTString), Property(Var("n")(CTNode), propertyKeyByName("name"))(CTString),
            logicalNodeScan("n", "Person")
          )
        )
      )
    )
    val orderedContents = result.header.slots.map(_.content).collect { case content: FieldSlotContent => content.key }

    result should equal(
      mkFlat.select(
        IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger.nullable)),
        mkFlat.project(
          ProjectedField(Var("baz")(CTInteger.nullable), Property(Var("n")(CTNode), propertyKeyByName("age"))(CTInteger.nullable)),
          mkFlat.project(ProjectedField(Var("foo")(CTString), Property(Var("n")(CTNode), propertyKeyByName("name"))(CTString)),
            flatNodeScan(Var("n")(CTNode), "Person")
          )
        )
      )
    )
    orderedContents should equal(IndexedSeq(Var("foo")(CTString), Var("n")(CTNode), Var("baz")(CTInteger)))
  }

  private def logicalNodeScan(nodeField: String, labelNames: String*) = {
    val labels = labelNames.map(Label)

    mkLogical.planNodeScan(Field(nodeField)(CTNode), EveryNode(AllOf(labels: _*)), logicalStartOperator)
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
