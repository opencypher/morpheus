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
package org.opencypher.caps.impl.common

import org.scalatest.{FunSuite, Matchers}

class TreeTest extends FunSuite with Matchers {

  val calculation = Tree(Add, Seq(Tree(Number(3)), Tree(Add, Seq(Tree(Number(5)), Tree(Number(4))))))
  val numberTree = calculation.map((_: Expr) => 1)
  val leaf = Tree(1)

  test("aggregate") {
    Calculate(calculation) should equal(12)
  }

  test("map and aggregate") {
    val numberTree = calculation.map((_: Expr) => 1)
    numberTree should equal(Tree(1, Seq(Tree(1), Tree(1, Seq(Tree(1), Tree(1))))))
    val treeSum = numberTree.aggregate { (i: Int, is: Seq[Int]) => i + is.sum }
    treeSum should equal(5)
  }

  test("rewrite") {
    val rewritten = InsertNopBeforeNumber(calculation)
    rewritten should equal(
      Tree(Add, Seq(
        Tree(Nop, Seq(Tree(Number(3)))),
        Tree(Add, Seq(Tree(Nop, Seq(Tree(Number(5)))), Tree(Nop, Seq(Tree(Number(4)))))))))
  }

  test("leaf") {
    calculation.isLeaf should equal(false)
    Tree(1).isLeaf should equal(true)
  }

  test("inner") {
    calculation.isInner should equal(true)
    leaf.isInner should equal(false)
  }

  test("arity") {
    calculation.arity should equal(2)
    leaf.arity should equal(0)
  }

  test("height") {
    calculation.height should equal(3)
    leaf.height should equal(1)
  }

  test("map and foldLeft") {
    val sum = numberTree.foldLeft(0){ case (s, n) => s + n }
    sum should equal(5)
  }

  test("toSeq") {
    numberTree.toSeq.toSet should equal(Set(1))
  }

  test("foreach") {
    var c = 0
    numberTree.foreach(_ => c += 1)
    c should equal(5)
  }


  case object Calculate extends Tree.Aggregate[Expr, Int] {
    override def apply(operator: Expr, inputs: Seq[Int]) = operator.eval(inputs: _*)
  }

  case object Nop extends Expr {
    override def eval(in: Int*) = in(0)
  }

  object InsertNopBeforeNumber extends Tree.ReplaceChildren[Expr] {
    override def shouldUpdate(parent: Tree[Expr]) = parent.value != Nop

    override def shouldReplace(childNode: Tree[Expr]) = childNode.value.isInstanceOf[Number]

    override def replace(childNode: Tree[Expr]) = Tree(Nop, Seq(childNode))
  }

  trait Expr {
    def eval(in: Int*): Int
  }

  case object Add extends Expr {
    def eval(in: Int*): Int = in.sum
  }

  case class Number(v: Int) extends Expr {
    override def eval(in: Int*) = v
  }

}
