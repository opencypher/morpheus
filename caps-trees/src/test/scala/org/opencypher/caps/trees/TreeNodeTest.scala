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
package org.opencypher.caps.trees

import org.scalatest.{FunSuite, Matchers}

class TreeNodeTest extends FunSuite with Matchers {

  val calculation = Add(Number(5), Add(Number(4), Number(3)))

  val leaf = Number(1)

  test("aggregate") {
    calculation.eval should equal(12)
  }

  test("leaf") {
    calculation.isLeaf should equal(false)
    leaf.isLeaf should equal(true)
  }

  test("arity") {
    calculation.arity should equal(2)
    leaf.arity should equal(0)
  }

  test("height") {
    calculation.height should equal(3)
    leaf.height should equal(1)
  }

  test("lists of children") {
    val addList1 = AddList(List(1), Number(1), 2, List(Number(2)), List[Object]("a", "b"))
    addList1.eval should equal(3)
    val addList2 = AddList(List(1), Number(1), 2, List(Number(2), Number(3)), List[Object]("a", "b"))
    addList2.eval should equal(6)
    val addList3 =
      AddList(List(1), Number(0), 2, List(Number(2)), List[Object]("a", "b"))
        .withNewChildren(Array(1, 2, 3, 4, 5, 6, 7).map(Number(_)))
    addList3 should equal(AddList(List(1), Number(1), 2, List(2, 3, 4, 5, 6, 7).map(Number(_)), List[Object]("a", "b")))
    addList3.eval should equal(28)
  }

  test("unsupported uses of lists of children") {
    // Test errors when violating list requirements

    // - a list of children cannot be empty
    intercept[IllegalArgumentException] {
      val fail = AddList(List(1), Number(1), 2, List.empty[Number], List[Object]("a", "b"))
      fail.children.toSet should equal(Set(Number(1)))
      fail.withNewChildren(Array(Number(1), Number(2)))
    }.getMessage should equal(
      "requirement failed: invalid number of children or used an empty list of children in the original node.")

    intercept[IllegalArgumentException] {
      val fail = AddList(List(1), Number(1), 2, List(Number(2)), List[Object]("a", "b"))
      fail.children.toSet should equal(Set(Number(1), Number(2)))
      fail.withNewChildren(Array(Number(1)))
    }.getMessage should equal("requirement failed: a list of children cannot be empty.")

    // - if any children are contained in a list at all, then all list elements need to be children
    intercept[InvalidConstructorArgument] {
      case class Unsupported(elems: List[Object]) extends AbstractTreeNode[Unsupported]
      val fail = Unsupported(List(Unsupported(List.empty), "2"))
    }.getMessage should equal(s"""Expected a list that contains either no children or only children
         |but found a mixed list that contains a child as the head element,
         |but also one with a non-child type: java.lang.String cannot be cast to ${classOf[AbstractTreeNode[_]].getName}.
         |""".stripMargin)

    // - there can be at most one list of children
    intercept[IllegalArgumentException] {
      abstract class UnsupportedTree extends AbstractTreeNode[UnsupportedTree]
      case object UnsupportedLeaf extends UnsupportedTree
      case class UnsupportedNode(elems1: List[UnsupportedTree], elems2: List[UnsupportedTree]) extends UnsupportedTree
      UnsupportedNode(List(UnsupportedLeaf), List(UnsupportedLeaf))
    }.getMessage should equal("requirement failed: there can be at most one list of children in the constructor.")

    // - there can be no normal child constructor parameters after the list of children
    intercept[IllegalArgumentException] {
      abstract class UnsupportedTree2 extends AbstractTreeNode[UnsupportedTree2]
      case object UnsupportedLeaf2 extends UnsupportedTree2
      case class UnsupportedNode2(elems: List[UnsupportedTree2], elem: UnsupportedTree2) extends UnsupportedTree2
      UnsupportedNode2(List(UnsupportedLeaf2), UnsupportedLeaf2)
    }.getMessage should equal(
      "requirement failed: there can be no normal child constructor parameters " +
        "after a list of children.")
  }

  test("rewrite") {
    val addNoops: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(n1: Number, n2: Number) => Add(NoOp(n1), NoOp(n2))
      case Add(n1: Number, n2)         => Add(NoOp(n1), n2)
      case Add(n1, n2: Number)         => Add(n1, NoOp(n2))
    }

    val expected = Add(NoOp(Number(5)), Add(NoOp(Number(4)), NoOp(Number(3))))
    val down = TopDown[CalcExpr](addNoops).rewrite(calculation)
    down should equal(expected)

    val up = BottomUp[CalcExpr](addNoops).rewrite(calculation)
    up should equal(expected)
  }

  test("support relatively high trees without stack overflow") {
    val highTree = (1 to 1000).foldLeft(Number(1): CalcExpr) {
      case (t, n) => Add(t, Number(n))
    }
    val simplified = BottomUp[CalcExpr] {
      case Add(Number(n1), Number(n2)) => Number(n1 + n2)
    }.rewrite(highTree)
    simplified should equal(Number(500501))

    val addNoOpsBeforeLeftAdd: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(a: Add, b) => Add(NoOp(a), b)
    }
    val noOpTree = TopDown[CalcExpr] {
      addNoOpsBeforeLeftAdd
    }.rewrite(highTree)
    noOpTree.height should equal(2000)
  }

  test("arg string") {
    Number(12).argString should equal("12")
    Add(Number(1), Number(2)).argString should equal("")
  }

  test("to string") {
    Number(12).toString should equal("Number(12)")
    Add(Number(1), Number(2)).toString should equal("Add")
  }

  test("pretty") {
    calculation.pretty should equal("""#|-Add
                                       #· |-Number(5)
                                       #· |-Add
                                       #· · |-Number(4)
                                       #· · |-Number(3)
                                       #""".stripMargin('#'))
  }

  test("copy with the same children returns the same instance") {
    calculation.withNewChildren(Array(calculation.left, calculation.right)) should be theSameInstanceAs calculation
  }

  abstract class CalcExpr extends AbstractTreeNode[CalcExpr] {
    def eval: Int
  }

  case class AddList(dummy1: List[Int], first: CalcExpr, dummy2: Int, remaining: List[CalcExpr], dummy3: List[Object])
      extends CalcExpr {
    def eval = first.eval + remaining.map(_.eval).sum
  }

  case class Add(left: CalcExpr, right: CalcExpr) extends CalcExpr {
    def eval = left.eval + right.eval
  }

  case class Number(v: Int) extends CalcExpr {
    def eval = v
  }

  case class NoOp(in: CalcExpr) extends CalcExpr {
    def eval = in.eval
  }

}
