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
package org.opencypher.spark.impl.acceptance

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.impl.MorpheusRecords
import org.opencypher.spark.testing.MorpheusTestSuite

class DrivingTableTests extends MorpheusTestSuite with ScanGraphInit {

  val data: util.List[Row] = List(
    Row(10, "Alice"),
    Row(20, "Bob"),
    Row(15, "Carol")
  ).asJava

  val schema = StructType(Seq(
    StructField("age", IntegerType),
    StructField("name", StringType)
  ))

  val drivingTable: MorpheusRecords = morpheus.records.wrap(morpheus.sparkSession.createDataFrame(data, schema))

  describe("simple usages") {
    it("return data from the driving table") {
      morpheus.cypher(
        """
          |RETURN age, name
        """.stripMargin, drivingTable = Some(drivingTable)).records.toMaps should equal(Bag(
        CypherMap("age" -> 10, "name" -> "Alice"),
        CypherMap("age" -> 20, "name" -> "Bob"),
        CypherMap("age" -> 15, "name" -> "Carol")
      ))
    }

    it("can combine driving table with unwind") {
      morpheus.cypher(
        """
          |UNWIND [1,2] AS i
          |RETURN i, age, name
        """.stripMargin, drivingTable = Some(drivingTable)).records.toMaps should equal(Bag(
        CypherMap("i" -> 1, "age" -> 10, "name" -> "Alice"),
        CypherMap("i" -> 1, "age" -> 20, "name" -> "Bob"),
        CypherMap("i" -> 1, "age" -> 15, "name" -> "Carol"),
        CypherMap("i" -> 2, "age" -> 10, "name" -> "Alice"),
        CypherMap("i" -> 2, "age" -> 20, "name" -> "Bob"),
        CypherMap("i" -> 2, "age" -> 15, "name" -> "Carol")
      ))
    }
  }

  describe("matching on driving table") {
    it("can use driving table data for filters") {
      val graph = initGraph(
        """
          |CREATE (:Person {name: "George", age: 20})
          |CREATE (:Person {name: "Frank", age: 50})
          |CREATE (:Person {name: "Jon", age: 15})
        """.stripMargin)

      graph.cypher(
        """
          |MATCH (p:Person)
          |WHERE p.age = age
          |RETURN p.name, name
        """.stripMargin, drivingTable = Some(drivingTable)).records.toMaps should equal(Bag(
        CypherMap("p.name" -> "George", "name" -> "Bob"),
        CypherMap("p.name" -> "Jon", "name" -> "Carol")
      ))
    }

    it("can use driving table data for filters inside the pattern") {
      val graph = initGraph(
        """
          |CREATE (:Person {name: "George", age: 20})
          |CREATE (:Person {name: "Frank", age: 50})
          |CREATE (:Person {name: "Jon", age: 15})
        """.stripMargin)

      graph.cypher(
        """
          |MATCH (p:Person {age: age})
          |RETURN p.name, name
        """.stripMargin, drivingTable = Some(drivingTable)).records.toMaps should equal(Bag(
        CypherMap("p.name" -> "George", "name" -> "Bob"),
        CypherMap("p.name" -> "Jon", "name" -> "Carol")
      ))
    }

    it("can use driving table for more complex matches") {
      val graph = initGraph(
        """
          |CREATE (b:B)
          |CREATE (:Person {name: "George", age: 20})-[:REL]->(b)
          |CREATE (:Person {name: "Frank", age: 50})-[:REL]->(b)
          |CREATE (:Person {name: "Jon", age: 15})-[:REL]->(b)
        """.stripMargin)

      graph.cypher(
        """
          |MATCH (p:Person {age: age})
          |MATCH (p)-[]->()
          |RETURN p.name, name
        """.stripMargin, drivingTable = Some(drivingTable)).records.toMaps should equal(Bag(
        CypherMap("p.name" -> "George", "name" -> "Bob"),
        CypherMap("p.name" -> "Jon", "name" -> "Carol")
      ))
    }
  }
}
