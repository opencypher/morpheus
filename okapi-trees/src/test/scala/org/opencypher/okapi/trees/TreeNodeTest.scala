/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.trees

import cats.data.NonEmptyList
import org.scalatest.{FunSpec, Matchers}

class TreeNodeTest extends FunSpec with Matchers {

  val calculation = Add(Number(5), Add(Number(4), Number(3)))

  val leaf = Number(1)

  it("aggregate") {
    calculation.eval should equal(12)
  }

  it("leaf") {
    calculation.isLeaf should equal(false)
    leaf.isLeaf should equal(true)
  }

  it("arity") {
    calculation.arity should equal(2)
    leaf.arity should equal(0)
  }

  it("height") {
    calculation.height should equal(3)
    leaf.height should equal(1)
  }

  it("lists of children") {
    val addList1 = AddList(NonEmptyList.one(1), Number(1), 2, NonEmptyList.one(Number(2)), NonEmptyList.of[Object]("a", "b"))
    addList1.eval should equal(3)
    val addList2 = AddList(NonEmptyList.one(1), Number(1), 2, NonEmptyList.of(Number(2), Number(3)), NonEmptyList.of[Object]("a", "b"))
    addList2.eval should equal(6)
    val addList3 =
      AddList(NonEmptyList.one(1), Number(0), 2, NonEmptyList.one(Number(2)), NonEmptyList.of[Object]("a", "b"))
        .withNewChildren(Array(1, 2, 3, 4, 5, 6, 7).map(Number))
    addList3 should equal(AddList(NonEmptyList.one(1), Number(1), 2, NonEmptyList.of(2, 3, 4, 5, 6, 7).map(Number), NonEmptyList.of[Object]("a", "b")))
    addList3.eval should equal(28)
  }

  it("unsupported uses of automatically detected children") {
    // Test errors when violating list requirements

    intercept[IllegalArgumentException] {
      val fail = AddList(NonEmptyList.one(1), Number(1), 2, NonEmptyList.one(Number(2)), NonEmptyList.of[Object]("a", "b"))
      fail.children.toSet should equal(Set(Number(1), Number(2)))
      fail.withNewChildren(Array(Number(1)))
    }.getMessage should equal("Cannot create NonEmptyList from empty list")

  }

  it("rewrite") {
    val addNoops: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(n1: Number, n2: Number) => Add(NoOp(n1), NoOp(n2))
      case Add(n1: Number, n2) => Add(NoOp(n1), n2)
      case Add(n1, n2: Number) => Add(n1, NoOp(n2))
    }

    val expected = Add(NoOp(Number(5)), Add(NoOp(Number(4)), NoOp(Number(3))))
    val down = TopDown[CalcExpr](addNoops).transform(calculation)
    down should equal(expected)

    val up = BottomUp[CalcExpr](addNoops).transform(calculation)
    up should equal(expected)
  }

  it("rewrites with context") {
    val sumOnce: PartialFunction[(CalcExpr, Boolean), (CalcExpr, Boolean)] = {
      case (Add(n1: Number, n2: Number), false) => Number(n1.v + n2.v) -> true
    }

    val expected = Add(Number(5), Number(7)) -> true

    val up = BottomUpWithContext(sumOnce).transform(calculation, false)
    up should equal(expected)
  }

  it("support relatively high trees without stack overflow") {
    val highTree = (1 to 1000).foldLeft(Number(1): CalcExpr) {
      case (t, n) => Add(t, Number(n))
    }
    val simplified = BottomUp[CalcExpr] {
      case Add(Number(n1), Number(n2)) => Number(n1 + n2)
    }.transform(highTree)
    simplified should equal(Number(500501))

    val addNoOpsBeforeLeftAdd: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(a: Add, b) => Add(NoOp(a), b)
    }
    val noOpTree = TopDown[CalcExpr] {
      addNoOpsBeforeLeftAdd
    }.transform(highTree)
    noOpTree.height should equal(2000)
  }

  it("stack safe rewrite") {
    val addNoops: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(n1: Number, n2: Number) => Add(NoOp(n1), NoOp(n2))
      case Add(n1: Number, n2) => Add(NoOp(n1), n2)
      case Add(n1, n2: Number) => Add(n1, NoOp(n2))
    }

    val expected = Add(NoOp(Number(5)), Add(NoOp(Number(4)), NoOp(Number(3))))

    val up = BottomUpStackSafe[CalcExpr](addNoops).transform(calculation)
    up should equal(expected)
  }

  it("support arbitrarily high high trees with stack safe rewrites") {
    val height = 50000
    val highTree = (1 to height).foldLeft(Number(1): CalcExpr) {
      case (t, n) => Add(t, Number(n))
    }
    BottomUpStackSafe[CalcExpr] {
      case Add(Number(n1), Number(n2)) => Number(n1 + n2)
    }.transform(highTree)

    val addNoOpsBeforeLeftAdd: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(a: Add, b) => Add(NoOp(a), b)
    }
    val noOpTree = TopDownStackSafe[CalcExpr] {
      addNoOpsBeforeLeftAdd
    }.transform(highTree)
    noOpTree.height should equal(2 * height)
  }

  it("arg string") {
    Number(12).argString should equal("v=12")
    Add(Number(1), Number(2)).argString should equal("")
  }

  it("option and list arg string") {
    Dummy(None, List.empty, None, List.empty).argString should equal("print1=None, print2=List()")
  }

  it("to string") {
    Number(12).toString should equal("Number(v=12)")
    Add(Number(1), Number(2)).toString should equal("Add")
  }

  it("pretty") {
    val t = Add(Add(Number(4), Number(3)), Add(Number(4), Number(3)))
    t.pretty should equal(
      """|╙──Add
         |    ╟──Add
         |    ║   ╟──Number(v=4)
         |    ║   ╙──Number(v=3)
         |    ╙──Add
         |        ╟──Number(v=4)
         |        ╙──Number(v=3)""".stripMargin)
  }

  it("copy with the same children returns the same instance") {
    calculation.withNewChildren(Array(calculation.left, calculation.right)) should be theSameInstanceAs calculation
  }

  abstract class UnsupportedTree extends AbstractTreeNode[UnsupportedTree]

  case class Unsupported(elems: NonEmptyList[Object]) extends UnsupportedTree

  case object UnsupportedLeaf extends UnsupportedTree

  case class UnsupportedNode(
    elems1: NonEmptyList[UnsupportedTree],
    elems2: NonEmptyList[UnsupportedTree]
  ) extends UnsupportedTree

  abstract class UnsupportedTree2 extends AbstractTreeNode[UnsupportedTree2]

  case object UnsupportedLeaf2 extends UnsupportedTree2

  case class UnsupportedNode2(elems: NonEmptyList[UnsupportedTree2], elem: UnsupportedTree2) extends UnsupportedTree2

  abstract class CalcExpr extends AbstractTreeNode[CalcExpr] {
    def eval: Int
  }

  case class Dummy(
    print1: Option[String],
    print2: List[String],
    dontPrint1: Option[CalcExpr],
    dontPrint2: List[CalcExpr]
  ) extends CalcExpr {
    def eval = 0
  }

  case class AddList(
    dummy1: NonEmptyList[Int],
    first: CalcExpr,
    dummy2: Int,
    remaining: NonEmptyList[CalcExpr],
    dummy3: NonEmptyList[Object]
  )
    extends CalcExpr {
    def eval: Int = first.eval + remaining.map(_.eval).toList.sum
  }

  case class Add(left: CalcExpr, right: CalcExpr) extends CalcExpr {
    def eval: Int = left.eval + right.eval
  }

  case class Number(v: Int) extends CalcExpr {
    def eval: Int = v
  }

  case class NoOp(in: CalcExpr) extends CalcExpr {
    def eval: Int = in.eval
  }


  it("can infer children for complex case classes") {
    val instance = Multi(
      NonEmptyList.one(1),
      NonEmptyList.of(SimpleA(), SimpleA()),
      List("A", "B"),
      List(SimpleB(), SimpleB()),
      Some(1L),
      None,
      Some(SimpleD())
    )

    instance.children.toList should equal(List(
      SimpleA(), SimpleA(), SimpleB(), SimpleB(), SimpleD()
    ))

    instance.withNewChildren(Array(
      SimpleA(), SimpleC()
    )) should equal(Multi(
      NonEmptyList.one(1),
      NonEmptyList.of(SimpleA()),
      List("A", "B"),
      List(),
      Some(1L),
      Some(SimpleC()),
      None
    ))
  }

  abstract class ComplexExample extends AbstractTreeNode[ComplexExample]

  case class SimpleA() extends ComplexExample

  case class SimpleB() extends ComplexExample

  case class SimpleC() extends ComplexExample

  case class SimpleD() extends ComplexExample

  case class Multi(
    ints: NonEmptyList[Integer],
    as: NonEmptyList[SimpleA],
    s: List[String],
    bs: List[SimpleB],
    maybeL: Option[Long],
    maybeC: Option[SimpleC],
    maybeD: Option[SimpleD]
  ) extends ComplexExample

}
