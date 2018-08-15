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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class FunctionsBehaviour extends CAPSTestSuite with DefaultGraphInit {

   describe("exists") {

    it("exists()") {
      val given = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

      val result = given.cypher("MATCH (n) RETURN exists(n.id) AS exists")

      result.records.toMaps should equal(
        Bag(
          CypherMap("exists" -> true),
          CypherMap("exists" -> true),
          CypherMap("exists" -> false),
          CypherMap("exists" -> false)
        ))
    }
  }

  describe("type") {

    it("type()") {
      val given = initGraph("CREATE ()-[:KNOWS]->()-[:HATES]->()-[:REL]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN type(r)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("type(r)" -> "KNOWS"),
          CypherMap("type(r)" -> "HATES"),
          CypherMap("type(r)" -> "REL")
        ))
    }
  }

  describe("id") {

    it("id for node") {
      val given = initGraph("CREATE (),()")

      val result = given.cypher("MATCH (n) RETURN id(n)")

      result.records.toMaps should equal(Bag(CypherMap("id(n)" -> 0), CypherMap("id(n)" -> 1)))
    }

    it("id for rel") {
      val given = initGraph("CREATE ()-[:REL]->()-[:REL]->()")

      val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

      result.records.toMaps should equal(Bag(CypherMap("id(e)" -> 2), CypherMap("id(e)" -> 4)))
    }

  }

  describe("labels") {

    it("get single label") {
      val given = initGraph("CREATE (:A), (:B)")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A")),
          CypherMap("labels(a)" -> List("B"))
        ))
    }

    it("get multiple labels") {
      val given = initGraph("CREATE (:A:B), (:C:D)")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A", "B")),
          CypherMap("labels(a)" -> List("C", "D"))
        ))
    }

    it("unlabeled nodes") {
      val given = initGraph("CREATE (:A), (:C:D), ()")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A")),
          CypherMap("labels(a)" -> List("C", "D")),
          CypherMap("labels(a)" -> List.empty)
        ))
    }

  }

  describe("size") {

    it("size() on literal list") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () RETURN size(['Alice', 'Bob']) as s")

      result.records.toMaps should equal(
        Bag(
          CypherMap("s" -> 2)
        ))
    }

    it("size() on literal string") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () RETURN size('Alice') as s")

      result.records.toMaps should equal(
        Bag(
          CypherMap("s" -> 5)
        ))
    }

    it("size() on retrieved string") {
      val given = initGraph("CREATE ({name: 'Alice'})")

      val result = given.cypher("MATCH (a) RETURN size(a.name) as s")

      result.records.toMaps should equal(
        Bag(
          CypherMap("s" -> 5)
        ))
    }

    it("size() on constructed list") {
      val given = initGraph("CREATE (:A:B), (:C:D), (:A), ()")

      val result = given.cypher("MATCH (a) RETURN size(labels(a)) as s")

      result.records.toMaps should equal(
        Bag(
          CypherMap("s" -> 2),
          CypherMap("s" -> 2),
          CypherMap("s" -> 1),
          CypherMap("s" -> 0)
        ))
    }

    ignore("size() on null") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH (a) RETURN size(a.prop) as s")

      result.records.toMaps should equal(Bag(CypherMap("s" -> null)))
    }

  }

  describe("keys") {

    it("keys()") {
      val given = initGraph("CREATE ({name:'Alice', age: 64, eyes:'brown'})")

      val result = given.cypher("MATCH (a) WHERE a.name = 'Alice' RETURN keys(a) as k")

      val keysAsMap = result.records.toMaps

      keysAsMap should equal(
        Bag(
          CypherMap("k" -> List("age", "eyes", "name"))
        ))
    }

    it("keys() does not return keys of unset properties") {
      val given = initGraph(
        """
          |CREATE (:Person {name:'Alice', age: 64, eyes:'brown'})
          |CREATE (:Person {name:'Bob', eyes:'blue'})
        """.stripMargin)

      val result = given.cypher("MATCH (a: Person) WHERE a.name = 'Bob' RETURN keys(a) as k")

      result.records.toMaps should equal(
        Bag(
          CypherMap("k" -> List("eyes", "name"))
        ))
    }

    // TODO: Enable when "Some error in type inference: Don't know how to type MapExpression" is fixed
    ignore("keys() works with literal maps") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () WITH {person: {name: 'Anne', age: 25}} AS p RETURN keys(p) as k")

      result.records.toMaps should equal(
        Bag(
          CypherMap("k" -> List("age", "name"))
        ))
    }

  }

  describe("startNode") {

    it("startNode()") {
      val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

      val result = given.cypher("MATCH ()-[r:FOO]->() RETURN r.val, startNode(r)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("r.val" -> "a", "startNode(r)" -> 0),
          CypherMap("r.val" -> "b", "startNode(r)" -> 3)
        ))
    }
  }

  describe("endNode") {

    it("endNode()") {
      val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

      val result = given.cypher("MATCH (a)-[r]->() RETURN r.val, endNode(r)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("r.val" -> "a", "endNode(r)" -> 1),
          CypherMap("r.val" -> "b", "endNode(r)" -> 4)
        ))
    }
  }

  describe("toFloat") {

    it("toFloat from integers") {
      val given = initGraph("CREATE (a {val: 1})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.records.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 1.0)
        ))
    }

    it("toFloat from float") {
      val given = initGraph("CREATE (a {val: 1.0d})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.records.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 1.0)
        ))
    }

    it("toFloat from string") {
      val given = initGraph("CREATE (a {val: '42'})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.records.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 42.0)
        ))
    }
  }

  describe("toString") {

    it("toString() on existing integer property") {
      val given = initGraph("CREATE ({id: 1}), ({id: 2})")

      val result = given.cypher("MATCH (n) RETURN toString(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> "1"),
          CypherMap("nId" -> "2")
        )
      )
    }

    it("toString() on existing float property") {
      val given = initGraph("CREATE ({id: 1.0}), ({id: 2.0})")

      val result = given.cypher("MATCH (n) RETURN toString(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> "1.0"),
          CypherMap("nId" -> "2.0")
        )
      )
    }

    it("toString() on existing boolean property") {
      val given = initGraph("CREATE ({id: true}), ({id: false})")

      val result = given.cypher("MATCH (n) RETURN toString(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> "true"),
          CypherMap("nId" -> "false")
        )
      )
    }

    it("toString() on existing string property") {
      val given = initGraph("CREATE ({id: 'true'}), ({id: 'false'})")

      val result = given.cypher("MATCH (n) RETURN toString(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> "true"),
          CypherMap("nId" -> "false")
        )
      )
    }

    it("toString() on non-existing properties") {
      val given = initGraph("CREATE ({id: 1}), ()")

      val result = given.cypher("MATCH (n) RETURN toString(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> "1"),
          CypherMap("nId" -> null)
        )
      )
    }
  }

  describe("toBoolean") {
    it("toBoolean() on valid literal string") {
      val given = initGraph("CREATE ({id: 'true'}), ({id: 'false'})")

      val result = given.cypher("MATCH (n) RETURN toBoolean(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> true),
          CypherMap("nId" -> false)
        )
      )
    }

    it("toBoolean() on booleans") {
      val given = initGraph("CREATE ({id: true}), ({id: false})")

      val result = given.cypher("MATCH (n) RETURN toBoolean(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> true),
          CypherMap("nId" -> false)
        )
      )
    }

    it("toBoolean() on invalid strings") {
      val given = initGraph("CREATE ({id: 'tr ue'}), ({id: 'fa lse'})")

      val result = given.cypher("MATCH (n) RETURN toBoolean(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> null),
          CypherMap("nId" -> null)
        )
      )
    }

    it("toBoolean() on missing property") {
      val given = initGraph("CREATE ({id: 'true'}), ()")

      val result = given.cypher("MATCH (n) RETURN toBoolean(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> true),
          CypherMap("nId" -> null)
        )
      )
    }
  }

  describe("coalesce") {
    it("can evaluate coalesce") {
      val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ({valC: 3}), ()")

      val result = given.cypher("MATCH (n) RETURN coalesce(n.valA, n.valB, n.valC) as value")

      result.records.collect.toBag should equal(
        Bag(
          CypherMap("value" -> 1),
          CypherMap("value" -> 2),
          CypherMap("value" -> 3),
          CypherMap("value" -> null)
        ))
    }

    it("can evaluate coalesce on non-existing expressions") {
      val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ()")

      val result = given.cypher("MATCH (n) RETURN coalesce(n.valD, n.valE) as value")

      result.records.collect.toBag should equal(
        Bag(
          CypherMap("value" -> null),
          CypherMap("value" -> null),
          CypherMap("value" -> null)
        ))
    }

  }

  describe("toInteger") {
    it("toInteger() on a graph") {
      val given = initGraph("CREATE (:Person {age: '42'})")

      val result = given.cypher(
        """
          |MATCH (n)
          |RETURN toInteger(n.age) AS age
        """.stripMargin)

      result.records.toMaps should equal(
        Bag(
          CypherMap("age" -> 42)
        )
      )
    }

    it("toInteger() on float") {
      val given = initGraph("CREATE (:Person {weight: '82.9'})")

      val result = given.cypher("MATCH (n) RETURN toInteger(n.weight) AS nWeight")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nWeight" -> 82)
        )
      )
    }

    it("toInteger() on invalid strings") {
      val given = initGraph("CREATE ({id: 'tr ue'}), ({id: ''})")

      val result = given.cypher("MATCH (n) RETURN toInteger(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> null),
          CypherMap("nId" -> null)
        )
      )
    }

    it("toInteger() on valid string") {
      val given = initGraph("CREATE ({id: '17'})")

      val result = given.cypher("MATCH (n) RETURN toInteger(n.id) AS nId")

      result.records.toMaps should equal(
        Bag(
          CypherMap("nId" -> 17)
        )
      )
    }
  }

  describe("sqrt") {
    it("on float value") {

      val result = caps.cypher("RETURN sqrt(12.96)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("sqrt(12.96)" -> 3.6)
        )
      )
    }

    it("on int value") {
      val result = caps.cypher("RETURN sqrt(9)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("sqrt(9)" -> 3.0)
        )
      )
    }
  }

  describe("log") {
    it("on float value") {

      val result = caps.cypher("RETURN log(12.96)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("log(12.96)" -> 2.561867690924129)
        )
      )
    }

    it("on int value") {
      val result = caps.cypher("RETURN log(9)")

      result.records.toMaps should equal(
        Bag(
          CypherMap("log(9)" -> 2.1972245773362196)
        )
      )
    }
  }
}
