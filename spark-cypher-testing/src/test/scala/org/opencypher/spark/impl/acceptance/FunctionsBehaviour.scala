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

import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class FunctionsBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("trim") {

    it("trim()") {
      val result = caps.cypher("RETURN trim('   hello  ') AS trimmed")
      result.records.toMaps should equal(
        Bag(
          CypherMap("trimmed" -> "hello")
        )
      )
    }

    it("ltrim()") {
      val result = caps.cypher("RETURN ltrim('   hello  ') AS trimmed")
      result.records.toMaps should equal(
        Bag(
          CypherMap("trimmed" -> "hello  ")
        )
      )
    }

    it("rtrim()") {
      val result = caps.cypher("RETURN rtrim('   hello  ') AS trimmed")
      result.records.toMaps should equal(
        Bag(
          CypherMap("trimmed" -> "   hello")
        )
      )
    }

    it("trims more complex structures") {
      val given = initGraph("CREATE ({name: ' foo '})")

      val result = given.cypher(
        """
          |MATCH (n)
          |WITH rtrim(n.name) AS name
          |RETURN rtrim(ltrim(name + '_bar ')) AS trimmed
        """.stripMargin)

      result.records.toMaps should equal(
        Bag(
          CypherMap("trimmed" -> "foo_bar")
        )
      )
    }
  }

  describe("timestamp") {

    it("is monotonically increasing") {
      val t1 = caps.cypher("RETURN timestamp()")
      val t2 = caps.cypher("RETURN timestamp()")

      t1.records.toMaps.keys.map(_.value.head._2.value.asInstanceOf[Long]) should be <=
        t2.records.toMaps.keys.map(_.value.head._2.value.asInstanceOf[Long])
    }

    it("should return the same value when called multiple times inside the same query") {
      val given = initGraph("CREATE (), ()")

      val result = given.cypher("WITH timestamp() AS t1 MATCH (n) RETURN t1, timestamp() AS t2")

      val expected = result.records.toMaps.head._1("t1")

      result.records.toMaps should equal(
        Bag(
          CypherMap("t1" -> expected, "t2" -> expected),
          CypherMap("t1" -> expected, "t2" -> expected)
        )
      )
    }

  }

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

  describe("logarithmic functions") {

    describe("sqrt()") {
      it("on float value") {

        val result = caps.cypher("RETURN sqrt(12.96) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 3.6)
          )
        )
      }

      it("on int value") {
        val result = caps.cypher("RETURN sqrt(9) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 3.0)
          )
        )
      }

      it("on null value") {
        val result = caps.cypher("RETURN sqrt(null) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> null)
          )
        )
      }
    }

    describe("log()") {
      it("on float value") {

        val result = caps.cypher("RETURN log(12.96) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 2.561867690924129)
          )
        )
      }

      it("on int value") {
        val result = caps.cypher("RETURN log(9) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 2.1972245773362196)
          )
        )
      }

      it("on null value") {
        val result = caps.cypher("RETURN log(null) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> null)
          )
        )
      }

    }

    describe("log10()") {
      it("on float value") {

        val result = caps.cypher("RETURN log10(12.96) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 1.1126050015345745)
          )
        )
      }

      it("on int value") {
        val result = caps.cypher("RETURN log10(100) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 2.0)
          )
        )
      }

      it("on null value") {
        val result = caps.cypher("RETURN log10(null) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> null)
          )
        )
      }
    }

    describe("exp()") {
      it("on float value") {

        val result = caps.cypher("RETURN exp(1.337) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 3.8076035433731965)
          )
        )
      }

      it("on int value") {
        val result = caps.cypher("RETURN exp(2) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> 7.38905609893065)
          )
        )
      }

      it("on null value") {
        val result = caps.cypher("RETURN exp(null) AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> null)
          )
        )
      }
    }

    describe("e()") {
      it("returns e") {

        val result = caps.cypher("RETURN e() AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> Math.E)
          )
        )
      }
    }

    describe("pi()") {
      it("returns pi") {

        val result = caps.cypher("RETURN pi() AS res")

        result.records.toMaps should equal(
          Bag(
            CypherMap("res" -> Math.PI)
          )
        )
      }
    }
  }

    describe("numeric functions") {

      describe("abs()") {

        it("on float value") {

          val result = caps.cypher("RETURN abs(-12.96) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 12.96)
            )
          )
        }

        it("on int value") {
          val result = caps.cypher("RETURN abs(-23) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 23)
            )
          )
        }

        it("on null value") {
          val result = caps.cypher("RETURN abs(null) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> null)
            )
          )
        }
      }

      describe("ceil()") {

        it("on float value") {

          val result = caps.cypher("RETURN ceil(0.1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1.0)
            )
          )
        }

        it("on int value") {
          val result = caps.cypher("RETURN ceil(1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1.0)
            )
          )
        }

        it("on null value") {
          val result = caps.cypher("RETURN ceil(null) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> null)
            )
          )
        }
      }

      describe("floor()") {

        it("on float value") {

          val result = caps.cypher("RETURN floor(1.9) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1.0)
            )
          )
        }

        it("on int value") {
          val result = caps.cypher("RETURN floor(1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1.0)
            )
          )
        }

        it("on null value") {
          val result = caps.cypher("RETURN floor(null) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> null)
            )
          )
        }
      }

      describe("rand()") {
        it("returns rand") {

          val result = caps.cypher("RETURN rand() AS res")

          val res = result.records.toMaps.head._1("res").cast[Double]
          res >= 0.0 shouldBe true
          res < 1.0 shouldBe true
        }
      }

      describe("round()") {

        it("on float value") {

          val result = caps.cypher("RETURN round(1.9) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 2.0)
            )
          )
        }

        it("on int value") {
          val result = caps.cypher("RETURN round(1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1.0)
            )
          )
        }

        it("on null value") {
          val result = caps.cypher("RETURN round(null) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> null)
            )
          )
        }
      }

      describe("sign()") {

        it("on float value") {

          val result = caps.cypher("RETURN sign(-1.1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> -1)
            )
          )
        }

        it("on int value") {
          val result = caps.cypher("RETURN sign(1) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> 1)
            )
          )
        }

        it("on null value") {
          val result = caps.cypher("RETURN sign(null) AS res")

          result.records.toMaps should equal(
            Bag(
              CypherMap("res" -> null)
            )
          )
        }
      }

    }

  describe("range") {
    it("can compute a range from literals") {
      caps.cypher(
        """UNWIND range(1, 3) AS x
          |RETURN x""".stripMargin).records.toMaps should equal(Bag(
        CypherMap("x" -> 1),
        CypherMap("x" -> 2),
        CypherMap("x" -> 3)
      ))
    }

    it("can compute a range from literals with custom steps") {
      caps.cypher(
        """UNWIND range(1, 7, 3) AS x
          |RETURN x""".stripMargin).records.toMaps should equal(Bag(
        CypherMap("x" -> 1),
        CypherMap("x" -> 4),
        CypherMap("x" -> 7)
      ))
    }

    it("can compute a range from column values") {
      val g = initGraph(
        """
          |CREATE (:A {from: 1, to: 2})
          |CREATE (:A {from: 1, to: 3})
          |CREATE (:A {from: 1, to: 4})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (n)
          |RETURN range(n.from, n.to) as x""".stripMargin).records.toMaps should equal(Bag(
        CypherMap("x" -> List(1, 2)),
        CypherMap("x" -> List(1, 2, 3)),
        CypherMap("x" -> List(1, 2, 3, 4))
      ))
    }

    it("can compute a range with varying step values") {
      val g = initGraph(
        """
          |CREATE (:A {step: 2})
          |CREATE (:A {step: 3})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (n)
          |RETURN range(1, 4, n.step) as x""".stripMargin).records.toMaps should equal(Bag(
        CypherMap("x" -> List(1, 3)),
        CypherMap("x" -> List(1, 4))
      ))
    }
  }

  describe("substring()") {

    it("returns substring from literal") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring('foobar', 3) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> "bar")
      ))
    }

    it("returns substring from literal with given length") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring('foobar', 0, 3) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> "foo")
      ))
    }

    it("returns substring from literal with exceeding given length") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring('foobar', 3, 10) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> "bar")
      ))
    }

    it("returns empty string for length 0") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring('foobar', 0, 0) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> "")
      ))
    }

    it("returns empty string for exceeding start") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring('foobar', 10) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> "")
      ))
    }

    it("returns null for null") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring(null, 0, 0) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> CypherNull)
      ))
    }

    it("throws for negative length") {
      val g = initGraph("CREATE ()")

      val result = g.cypher("RETURN substring(null, 0, 0) AS substring")

      result.records.toMaps should equal(Bag(
        CypherMap("substring" -> CypherNull)
      ))
    }
  }

  describe("negative tests") {
    it("should give a good error message on unimplemented functions") {
      the [NotImplementedException] thrownBy {
        caps.cypher("RETURN tail([1, 2])")
      } should have message "The expression FunctionInvocation(Namespace(List()),FunctionName(tail),false,Vector(Parameter(  AUTOLIST0,List<Any>))) [line 1, column 8 (offset: 7)] is not supported by the system"
    }
  }
}
