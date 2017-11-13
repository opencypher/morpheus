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
package org.opencypher.caps.ir.impl.block

import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.ir.api.block.MatchBlock
import org.opencypher.caps.ir.impl.IrTestSuite
import org.opencypher.caps.ir.impl.instances._

class TypedMatchBlockTest extends IrTestSuite {

  test("computes detailed type of pattern variables") {
    implicit val (block, globals) = matchBlock("MATCH (n:Person:Foo)-[r:TYPE]->(m) RETURN n")

    typedMatchBlock.outputs(block).map(_.toTypedTuple) should equal(
      Set(
        "n" -> CTNode("Person", "Foo"),
        "r" -> CTRelationship("TYPE"),
        "m" -> CTNode()
      ))
  }

  test("computes detailed type of entities also from WHERE clause") {
    implicit val (block, globals) = matchBlock("MATCH (n:Person:Foo)-[r:TYPE]->(m) WHERE n:Three RETURN n")

    typedMatchBlock.outputs(block).map(_.toTypedTuple) should equal(
      Set(
        "n" -> CTNode("Person", "Foo", "Three"),
        "r" -> CTRelationship("TYPE"),
        "m" -> CTNode()
      ))
  }

  // TODO: We need to register the string literal as a relationship type in globals extraction -- is this what we want
  ignore("computes detailed relationship type from WHERE clause") {
    implicit val (block, globals) = matchBlock("MATCH ()-[r]->() WHERE type(r) = 'TYPE' RETURN $noAutoParams")

    typedMatchBlock.outputs(block).map(_.toTypedTuple) should equal(
      Set(
        "r" -> CTRelationship("TYPE")
      ))
  }

  private def matchBlock(singleMatchQuery: String): (MatchBlock[Expr], Map[String, CypherValue]) = {
    val model = singleMatchQuery.ir.model
    val projectBlockRef = model.result.after.head
    val matchBlockRef = model.blocks(projectBlockRef).after.head

    model.blocks(matchBlockRef) match {
      case block: MatchBlock[Expr] =>
        block -> model.parameters

      case x => throw new MatchError(s"Supposed to be a match block, was: $x")
    }
  }
}
