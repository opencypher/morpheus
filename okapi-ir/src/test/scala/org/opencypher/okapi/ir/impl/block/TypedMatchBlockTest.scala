/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl.block

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api.block.MatchBlock
import org.opencypher.okapi.ir.impl.IrTestSuite
import org.opencypher.okapi.ir.impl.refactor.instances._

class TypedMatchBlockTest extends IrTestSuite {

  implicit val graph: Some[QualifiedGraphName] = Some(testQualifiedGraphName)

  it("computes detailed types of pattern variables") {
    implicit val (block, globals) = matchBlock("MATCH (n:Person:Foo)-[r:TYPE]->(m) RETURN n")

    typedMatchBlock.outputs(block).map(_.toTypedTuple) should equal(
      Set(
        "n" -> CTNode(Set("Person", "Foo"), Some(testQualifiedGraphName)),
        "r" -> CTRelationship(Set("TYPE"), Some(testQualifiedGraphName)),
        "m" -> CTNode(Set.empty[String], Some(testQualifiedGraphName))
      ))
  }

  test("computes detailed type of elements also from WHERE clause") {
    implicit val (block, globals) = matchBlock("MATCH (n:Person:Foo)-[r:TYPE]->(m) WHERE n:Three RETURN n")

    typedMatchBlock.outputs(block).map(_.toTypedTuple) should equal(
      Set(
        "n" -> CTNode(Set("Person", "Foo", "Three"), Some(testQualifiedGraphName)),
        "r" -> CTRelationship(Set("TYPE"), Some(testQualifiedGraphName)),
        "m" -> CTNode(Set.empty[String], Some(testQualifiedGraphName))
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

  private def matchBlock(singleMatchQuery: String): (MatchBlock, CypherMap) = {
    val model = singleMatchQuery.asCypherQuery().model
    val projectBlock = model.result.after.head
    val matchBlock = projectBlock.after.head

    matchBlock match {
      case block: MatchBlock =>
        block -> model.parameters

      case x => throw new MatchError(s"Supposed to be a match block, was: $x")
    }
  }
}
