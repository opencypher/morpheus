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

class TreeNodeTest extends FunSuite with Matchers {

  val calculation = Add(Number(5), Add(Number(4), Number(3)))

  val leaf = Number(1)

//  test("aggregate") {
//    Calculate(calculation) should equal(12)
//  }
//
//  test("map and aggregate") {
//    val numberTree = calculation.map((_: Expr) => 1)
//    numberTree should equal(Tree(1, Seq(Tree(1), Tree(1, Seq(Tree(1), Tree(1))))))
//    val treeSum = numberTree.aggregate { (i: Int, is: Seq[Int]) => i + is.sum }
//    treeSum should equal(5)
//  }
//
  test("leaf") {
    calculation.isLeaf should equal(false)
    leaf.isLeaf should equal(true)
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

  test("rewrite") {
    val addNoops: TreeNode.RewriteRule[Expr] = TreeNode.RewriteRule {
      case Add(n1: Number, n2: Number)  => Add(Noop(n1), Noop(n2))
      case Add(n1: Number, n2)  => Add(Noop(n1), n2)
      case Add(n1, n2: Number)  => Add(n1, Noop(n2))
    }
    println(calculation.transformUp(addNoops).pretty())

    println(calculation.transformDown(addNoops).pretty())
  }
//
//  test("map and foldLeft") {
//    val sum = numberTree.foldLeft(0){ case (s, n) => s + n }
//    sum should equal(5)
//  }
//
//  test("toSeq") {
//    numberTree.toSeq.toSet should equal(Set(1))
//  }
//
//  test("foreach") {
//    var c = 0
//    numberTree.foreach(_ => c += 1)
//    c should equal(5)
//  }
//
//  test("transform up") {
//    val aggregateAdds = Tree.Rewrite[Expr] {
//      case Tree(Add, Seq(Tree(Number(n1), _), Tree(Number(n2), _))) => Tree(Number(n1 + n2))
//    }
//
//    val addNoops = Tree.Rewrite[Expr] {
//      case t @ Tree(_ : Number, _) => Tree(Noop, Seq(t))
//    }
//
//    val replaceAddSub = Tree.Rewrite[Expr] {
//      case Tree(expr, Seq(sub @ Tree(Sub, _), others @ _*)) => {
//        val subval = sub.aggregate(Calculate)
//        Tree(expr, Seq(Tree(Number(subval))) ++ others)
//      }
//    }
//
//    calculation.transformUp(aggregateAdds) should equal(Tree(Number(12)))
//    calculation.transformUp(addNoops) should equal(
//      Tree(Add, Seq(
//        Tree(Noop, Seq(
//          Tree(Number(3))
//        )),
//        Tree(Add, Seq(
//          Tree(Noop, Seq(
//            Tree(Number(5))
//          )),
//          Tree(Noop, Seq(
//            Tree(Number(4))
//          ))
//        ))
//      ))
//    )
//  }

  abstract class Expr extends TreeNode[Expr]

  case class Add(left: Expr, right: Expr) extends Expr {

    override val children: Seq[Expr] = Seq(left, right)

    override def withNewChildren(newChildren: Seq[Expr]): Expr = copy(left = newChildren(0), right = newChildren(1))
  }

  case class Number(v: Int) extends Expr {
    override def withNewChildren(newChildren: Seq[Expr]): Expr = this
  }

  case class Noop(in: Expr) extends Expr {

    override val children: Seq[Expr] = Seq(in)

    override def withNewChildren(newChildren: Seq[Expr]): Expr = copy(in = newChildren.head)
  }
}
