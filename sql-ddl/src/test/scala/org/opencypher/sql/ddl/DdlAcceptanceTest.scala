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
package org.opencypher.sql.ddl

import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.sql.ddl.DdlParser._

class DdlAcceptanceTest extends BaseTestSuite {

  describe("DDL to OKAPI schema") {
    it("can construct schema with node label") {
      val gName = "test"

      val ddl =
        s"""CATALOG CREATE LABEL (A {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  (A)
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with edge label") {
      val gName = "test"

      val ddl =
        s"""CATALOG CREATE LABEL (A {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  [A]
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withRelationshipPropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with node and edge labels") {
      val gName = "test"

      val ddl =
        s"""
           |CATALOG CREATE LABEL (Node {val: String})
           |CATALOG CREATE LABEL (REL {name: STRING})
           |
           |CREATE GRAPH SCHEMA foo
           |  (Node) [REL]
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin


      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
      )
    }

    it("can construct schema with node labels with element key") {
      val gName = "test"

      val ddl =
        s"""
           |CATALOG CREATE LABEL (Node {val: String, another : String})
           |   KEY akey (val)
           |
           |CREATE GRAPH SCHEMA foo
           |  (Node)
           |
           |CREATE GRAPH $gName WITH SCHEMA foo
           |""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString, "another" -> CTString)
          .withNodeKey("Node", Set("val"))
      )
    }

    it("can construct schema with single NEN pattern") {
      val gName = "test"

      val ddl =
        s"""|CATALOG CREATE LABEL (Node {val: String})
            |CATALOG CREATE LABEL (REL {name: STRING})
            |
            |CREATE GRAPH SCHEMA foo
            |  (Node)-[REL]->(Node)
            |
            |CREATE GRAPH $gName WITH SCHEMA foo""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can construct schema with single NEN pattern 2") {
      val gName = "test"

      val ddl =
        s"""|CATALOG CREATE LABEL (Node {val: String})
            |CATALOG CREATE LABEL (REL {name: STRING})
            |
            |CREATE GRAPH SCHEMA foo
            | (Node)-[REL]->(Node)
            |
            |CREATE GRAPH $gName WITH SCHEMA foo""".stripMargin

      parse(ddl).graphSchemas(gName) should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can combine local and global labels") {
      // Given
      val ddl =
        """
          |CATALOG CREATE LABEL (MyLabel {property: STRING, data: INTEGER?})
          |CATALOG CREATE LABEL (REL_TYPE1 {property: BOOLEAN})
          |CATALOG CREATE LABEL (REL_TYPE2)
          |
          |CREATE GRAPH SCHEMA mySchema
          |  -- local label declarations
          |  LABEL (LocalLabel1 {property: STRING}),
          |  LABEL (LocalLabel2),
          |
          |  -- label set declarations
          |  (LocalLabel1, LocalLabel2),
          |  (LocalLabel1),
          |  (MyLabel),
          |
          |  [REL_TYPE1],
          |  [REL_TYPE2]
          |
          |  -- schema patterns
          |  (MyLabel) <0..*> -[REL_TYPE1]-> <1> (LocalLabel1),
          |  (LocalLabel1, LocalLabel2)-[REL_TYPE2]->(MyLabel)
        """.stripMargin

      parse(ddl).globalSchemas("mySchema") should equal(
        Schema.empty
          .withNodePropertyKeys("MyLabel")("property" -> CTString, "data" -> CTInteger.nullable)
          .withNodePropertyKeys("LocalLabel1")("property" -> CTString)
          .withNodePropertyKeys("LocalLabel1", "LocalLabel2")("property" -> CTString)
          .withRelationshipPropertyKeys("REL_TYPE1")("property" -> CTBoolean)
          .withRelationshipPropertyKeys("REL_TYPE2")()
          .withSchemaPatterns(SchemaPattern(Set("MyLabel"), "REL_TYPE1", Set("LocalLabel1")))
          .withSchemaPatterns(SchemaPattern(Set("LocalLabel1", "LocalLabel2"), "REL_TYPE2", Set("MyLabel")))
      )
    }
  }
  
}
