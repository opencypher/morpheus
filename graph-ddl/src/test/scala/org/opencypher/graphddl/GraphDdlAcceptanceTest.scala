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
package org.opencypher.graphddl

import org.opencypher.graphddl.GraphDdlParser._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing

class GraphDdlAcceptanceTest extends BaseTestSuite {

  val schemaName = "mySchema"
  val graphName = GraphName("myGraph")

  describe("DDL to OKAPI schema") {
    it("can construct schema with node label") {

      val ddl =
        s"""|CATALOG CREATE LABEL A ({name: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (A)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
          """.stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with edge label") {

      val ddl =
        s"""CATALOG CREATE LABEL A ({name: STRING})
           |
           |CREATE GRAPH SCHEMA $schemaName (
           |  [A]
           |)
           |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
           |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withRelationshipPropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with node and edge labels") {
      val ddl =
        s"""|CATALOG CREATE LABEL Node ({val: String})
            |CATALOG CREATE LABEL REL ({name: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (Node), [REL]
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin


      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
      )
    }

    it("prefers local label over global label") {

      val ddl =
        s"""|CATALOG CREATE LABEL Node ({val: String})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  LABEL Node ({ foo : Integer }),
            |  (Node)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin


      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty.withNodePropertyKeys("Node")("foo" -> CTInteger)
      )
    }

    it("can construct schema with node labels with element key") {
      val ddl =
        s"""|CATALOG CREATE LABEL Node ({val: String, another : String} KEY akey (val))
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (Node)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString, "another" -> CTString)
          .withNodeKey("Node", Set("val"))
      )
    }

    it("can construct schema with single NEN pattern") {
      val ddl =
        s"""|CATALOG CREATE LABEL Node ({val: String})
            |CATALOG CREATE LABEL REL ({name: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (Node)-[REL]->(Node)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can construct schema with single NEN pattern 2") {
      val ddl =
        s"""|CATALOG CREATE LABEL Node ({val: String})
            |CATALOG CREATE LABEL REL ({name: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            | (Node)-[REL]->(Node)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))

      )
    }

    it("can combine local and global labels") {
      // Given
      val ddl =
        s"""|CATALOG CREATE LABEL MyLabel ({property: STRING, data: INTEGER?})
            |CATALOG CREATE LABEL REL_TYPE1 ({property: BOOLEAN})
            |CATALOG CREATE LABEL REL_TYPE2
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  -- local label declarations
            |  LABEL LocalLabel1 ({property: STRING}),
            |  LABEL LocalLabel2,
            |
            |  -- label set declarations
            |  (LocalLabel1, LocalLabel2),
            |  (LocalLabel1),
            |  (MyLabel),
            |
            |  [REL_TYPE1],
            |  [REL_TYPE2],
            |
            |  -- schema patterns
            |  (MyLabel) <0..*> -[REL_TYPE1]-> <1> (LocalLabel1),
            |  (LocalLabel1, LocalLabel2)-[REL_TYPE2]->(MyLabel)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
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

    it("merges property keys for label combination") {
      // Given
      val ddl =
        s"""|CATALOG CREATE LABEL A ({foo: STRING})
            |CATALOG CREATE LABEL B ({bar: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (A),
            |  (A, B)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodePropertyKeys("A", "B")("foo" -> CTString, "bar" -> CTString)
      )
    }

    it("merges identical property keys with same type") {
      // Given
      val ddl =
        s"""|CATALOG CREATE LABEL A ({foo: STRING})
            |CATALOG CREATE LABEL B ({foo: STRING})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (A),
            |  (A, B)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType should equal(
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodePropertyKeys("A", "B")("foo" -> CTString)
      )
    }

    it("parses correct schema") {
      val ddlDefinition: DdlDefinition = parse(
        s"""|SET SCHEMA foo.bar;
            |
            |CATALOG CREATE LABEL A ({name: STRING})
            |
            |CATALOG CREATE LABEL B ({sequence: INTEGER, nationality: STRING?, age: INTEGER?})
            |
            |CATALOG CREATE LABEL TYPE_1
            |
            |CATALOG CREATE LABEL TYPE_2 ({prop: BOOLEAN?})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  LABEL A ({ foo : INTEGER } ),
            |  LABEL C,
            |
            |  -- nodes
            |  (A),
            |  (B),
            |  (A, B),
            |  (C),
            |
            |
            |  -- edges
            |  [TYPE_1],
            |  [TYPE_2],
            |
            |  -- schema patterns
            |  (A) <0 .. *> - [TYPE_1] -> <1> (B)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName (
            |
            |  (A) FROM foo,
            |
            |  [TYPE_1] FROM baz edge
            |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
            |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
            |)
            |""".stripMargin)

      ddlDefinition should equalWithTracing(
        DdlDefinition(List(
          SetSchemaDefinition("foo", "bar"),
          LabelDefinition("A", Map("name" -> CTString)),
          LabelDefinition("B", Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
          LabelDefinition("TYPE_1"),
          LabelDefinition("TYPE_2", Map("prop" -> CTBoolean.nullable)),
          GlobalSchemaDefinition(schemaName, SchemaDefinition(List(
            LabelDefinition("A", properties = Map("foo" -> CTInteger)),
            LabelDefinition("C"),
            NodeDefinition(Set("A")),
            NodeDefinition(Set("B")),
            NodeDefinition(Set("A", "B")),
            NodeDefinition(Set("C")),
            RelationshipDefinition("TYPE_1"),
            RelationshipDefinition("TYPE_2"),
            SchemaPatternDefinition(Set(Set("A")), CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)), Set(Set("B")))
          ))),
          GraphDefinition(
            name = graphName.value,
            maybeSchemaName = Some(schemaName),
            localSchemaDefinition = SchemaDefinition(),
            mappings = List(
              NodeMappingDefinition(NodeDefinition("A"), List(NodeToViewDefinition(List("foo")))),
              RelationshipMappingDefinition(RelationshipDefinition("TYPE_1"), List(RelationshipToViewDefinition(
                viewDefinition = ViewDefinition(List("baz"), "edge"),
                startNodeToViewDefinition = LabelToViewDefinition(
                  NodeDefinition("A"),
                  ViewDefinition(List("foo"), "alias_foo"),
                  JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
                endNodeToViewDefinition = LabelToViewDefinition(
                  NodeDefinition("B"),
                  ViewDefinition(List("bar"), "alias_bar"),
                  JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
          )))))
        ))
      )

      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys(Set("C"))
        .withRelationshipType("TYPE_1")
        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTBoolean.nullable)
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    }
  }

  describe("Exception cases") {

    it("throws when merging identical property keys with conflicting types") {
      // Given
      val ddl =
        s"""|CATALOG CREATE LABEL A ({foo: STRING})
            |CATALOG CREATE LABEL B ({foo: INTEGER})
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |  (A),
            |  (A, B)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(ddl).graphs(graphName)
      }
    }


    it("throws if a label is not defined") {
      val ddlDefinition = parse(
        s"""|CATALOG CREATE LABEL A
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |
            |  LABEL B,
            |
            |  -- (illegal) node definition
            |  (C)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin)


      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(ddlDefinition).graphs(graphName).graphType
      }
    }

    it("throws if a relationship type is not defined") {
      val ddlDefinition = parse(
        s"""|CATALOG CREATE LABEL A
            |
            |CREATE GRAPH SCHEMA $schemaName (
            |
            |  LABEL B,
            |
            |  -- (illegal) relationship type definition
            |  [C]
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin)

      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(ddlDefinition).graphs(graphName).graphType
      }
    }

    it("throws if a undefined label is used") {
      val ddlString =
        s"""|CREATE GRAPH SCHEMA $schemaName (
            |  (A)-[T]->(A)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName ()
            |""".stripMargin

      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(parse(ddlString)).graphs(graphName).graphType
      }
    }

    it("throws if an unknown property key is mapped to a column") {
      val ddlString =
        s"""|CREATE GRAPH SCHEMA $schemaName (
            |  LABEL A ({ foo: STRING }),
            |  (A)
            |)
            |CREATE GRAPH $graphName WITH GRAPH SCHEMA $schemaName (
            |  (A) FROM view_A ( column AS bar )
            |)
            |""".stripMargin

      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(parse(ddlString)).graphs(graphName).graphType
      }
    }
  }

}
