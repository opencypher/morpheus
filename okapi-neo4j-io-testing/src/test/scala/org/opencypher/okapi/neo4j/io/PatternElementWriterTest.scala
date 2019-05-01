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
package org.opencypher.okapi.neo4j.io

import org.neo4j.driver.v1.Values
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults.metaPropertyKey
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.testing.Bag._
import org.opencypher.okapi.testing.BaseTestSuite
import org.scalatest.BeforeAndAfter

import scala.collection.immutable

class PatternElementWriterTest extends BaseTestSuite with Neo4jServerFixture with BeforeAndAfter {

  it("can write nodes") {
    ElementWriter.createNodes(
      inputNodes.toIterator,
      Array(metaPropertyKey, "val1", "val2", "val3", null),
      neo4jConfig,
      Set("Foo", "Bar", "Baz")
    )(rowToListValue)

    val expected = inputNodes.map { node =>
      CypherMap(
        s"n.$metaPropertyKey" -> node(0),
        "n.val1" -> node(1),
        "n.val2" -> node(2),
        "n.val3" -> node(3)
      )
    }.toBag

    val result = neo4jConfig.cypherWithNewSession(s"MATCH (n) RETURN n.$metaPropertyKey, n.val1, n.val2, n.val3").map(CypherMap).toBag
    result should equal(expected)
  }

  it("can write relationships") {
    ElementWriter.createRelationships(
      inputRels.toIterator,
      1,
      2,
      Array(metaPropertyKey, null, null, "val3"),
      neo4jConfig,
      "REL",
      None
    )(rowToListValue)

    val expected = inputRels.map { rel =>
      CypherMap(
        s"r.$metaPropertyKey" -> rel(0),
        "r.val3" -> rel(3)
      )
    }.toBag

    val result = neo4jConfig.cypherWithNewSession(s"MATCH ()-[r]->() RETURN r.$metaPropertyKey, r.val3").map(CypherMap).toBag
    result should equal(expected)
  }

  override def dataFixture: String = ""

  private def rowToListValue(data: Array[AnyRef]) = Values.value(data.map(Values.value): _*)

  private val numberOfNodes = 10
  val inputNodes: immutable.IndexedSeq[Array[AnyRef]] = (1 to numberOfNodes).map { i =>
    Array[AnyRef](
      i.asInstanceOf[AnyRef],
      i.asInstanceOf[AnyRef],
      i.toString.asInstanceOf[AnyRef],
      (i % 2 == 0).asInstanceOf[AnyRef],
      (i+1).asInstanceOf[AnyRef]
    )
  }

  val inputRels: immutable.IndexedSeq[Array[AnyRef]] = (2 to numberOfNodes).map { i =>
    Array[AnyRef](
      i.asInstanceOf[AnyRef],
      (i - 1).asInstanceOf[AnyRef],
      i.asInstanceOf[AnyRef],
      (i % 2 == 0).asInstanceOf[AnyRef]
    )
  }
}
