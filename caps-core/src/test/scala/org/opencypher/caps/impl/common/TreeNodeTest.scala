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

  test("rewrite") {
    val addNoops: PartialFunction[CalcExpr, CalcExpr] = {
      case Add(n1: Number, n2: Number) => Add(Noop(n1), Noop(n2))
      case Add(n1: Number, n2)         => Add(Noop(n1), n2)
      case Add(n1, n2: Number)         => Add(n1, Noop(n2))
    }

    val expected = Add(Noop(Number(5)), Add(Noop(Number(4)), Noop(Number(3))))
    val down = calculation.transformDown(addNoops)
    down should equal(expected)

    val up = calculation.transformUp(addNoops)
    up should equal(expected)
  }

  abstract class CalcExpr extends AbstractTreeNode[CalcExpr] {
    def eval: Int
  }

  case class Add(left: CalcExpr, right: CalcExpr) extends CalcExpr {
    def eval = left.eval + right.eval
  }

  case class Number(v: Int) extends CalcExpr {
    def eval = v
  }

  case class Noop(in: CalcExpr) extends CalcExpr {
    def eval = in.eval
  }

}
