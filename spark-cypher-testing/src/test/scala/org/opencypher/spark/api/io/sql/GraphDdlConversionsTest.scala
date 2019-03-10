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
package org.opencypher.spark.api.io.sql

import org.opencypher.graphddl.GraphDdlParser.parseDdl
import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.spark.api.io.sql.GraphDdlConversions._

class GraphDdlConversionsTest extends BaseTestSuite {

  val typeName = "myType"
  val graphName = GraphName("myGraph")

  describe("GraphType to OKAPI schema") {

    it("converts a graph type with single element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          | Person ( name STRING, age INTEGER ),
          | Book   ( title STRING ) ,
          | READS  ( rating FLOAT ) ,
          | (Person),
          | (Book),
          | (Person)-[READS]->(Book)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
        .withNodePropertyKeys("Book")("title" -> CTString)
        .withRelationshipPropertyKeys("READS")("rating" -> CTFloat)
        .withSchemaPatterns(SchemaPattern("Person", "READS", "Book")))
    }

    it("converts a graph type with multiple element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A (x STRING),
          |  B (y STRING),
          |  R (y STRING),
          |  (A),
          |  (A, B),
          |  (A)-[R]->(A),
          |  (A, B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

    it("converts a graph type with element type inheritance") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A           (x STRING),
          |  B EXTENDS A (y STRING),
          |  R (y STRING),
          |  (A),
          |  (B),
          |  (A)-[R]->(A),
          |  (B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

    it("can construct schema with node label") {

      val ddl =
        s"""|CREATE ELEMENT TYPE A ( name STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (A)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
          """.stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
      )
    }

    it("can construct schema with edge label") {

      val ddl =
        s"""CREATE ELEMENT TYPE A ( name STRING )
           |
           |CREATE GRAPH TYPE $typeName (
           |  (A),
           |  (A)-[A]->(A)
           |)
           |CREATE GRAPH $graphName OF $typeName ()
           |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
          .withRelationshipPropertyKeys("A")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("A", "A", "A"))
      )
    }

    it("can construct schema with node and edge labels") {
      val ddl =
        s"""|CREATE ELEMENT TYPE Node1 ( val String )
            |CREATE ELEMENT TYPE Node2 ( val String )
            |CREATE ELEMENT TYPE REL ( name STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (Node1),
            |  (Node2),
            |  (Node1)-[REL]->(Node2)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin


      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema shouldEqual
        Schema.empty
          .withNodePropertyKeys("Node1")("val" -> CTString)
          .withNodePropertyKeys("Node2")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node1", "REL", "Node2"))

    }

    it("prefers local label over global label") {

      val ddl =
        s"""|CREATE ELEMENT TYPE Node ( val String )
            |
            |CREATE GRAPH TYPE $typeName (
            |  Node ( foo Integer ),
            |  (Node)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin


      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty.withNodePropertyKeys("Node")("foo" -> CTInteger)
      )
    }

    it("can construct schema with node labels with element key") {
      val ddl =
        s"""|CREATE ELEMENT TYPE Node ( val String, another String ) KEY akey (val)
            |
            |CREATE GRAPH TYPE $typeName (
            |  (Node)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString, "another" -> CTString)
          .withNodeKey("Node", Set("val"))
      )
    }

    it("can construct schema with single NEN pattern") {
      val ddl =
        s"""|CREATE ELEMENT TYPE Node ( val String )
            |CREATE ELEMENT TYPE REL ( name STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (Node),
            |  (Node)-[REL]->(Node)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("Node")("val" -> CTString)
          .withRelationshipPropertyKeys("REL")("name" -> CTString)
          .withSchemaPatterns(SchemaPattern("Node", "REL", "Node"))
      )
    }

    it("can combine local and global labels") {
      // Given
      val ddl =
        s"""|CREATE ELEMENT TYPE MyLabel ( property STRING, data INTEGER? )
            |CREATE ELEMENT TYPE REL_TYPE1 ( property BOOLEAN )
            |CREATE ELEMENT TYPE REL_TYPE2
            |
            |CREATE GRAPH TYPE $typeName (
            |  -- local label declarations
            |  LocalLabel1 ( property STRING ),
            |  LocalLabel2,
            |
            |  -- label set declarations
            |  (LocalLabel1, LocalLabel2),
            |  (LocalLabel1),
            |  (MyLabel),
            |
            |  -- schema patterns
            |  (MyLabel)-[REL_TYPE1]->(LocalLabel1),
            |  (LocalLabel1, LocalLabel2)-[REL_TYPE2]->(MyLabel)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
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
        s"""|CREATE ELEMENT TYPE A ( foo STRING )
            |CREATE ELEMENT TYPE B ( bar STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (A),
            |  (A, B)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodePropertyKeys("A", "B")("foo" -> CTString, "bar" -> CTString)
      )
    }

    it("merges property keys for label combination based on element type hierarchy") {
      // Given
      val ddl =
        s"""|CREATE ELEMENT TYPE A ( foo STRING )
            |CREATE ELEMENT TYPE B EXTENDS A ( bar STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (A),
            |  (B)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodePropertyKeys("A", "B")("foo" -> CTString, "bar" -> CTString)
      )
    }

    it("merges property keys for label combination based on element type with multi-inheritance") {
      // Given
      val ddl =
        s"""|CREATE ELEMENT TYPE A ( a STRING )
            |CREATE ELEMENT TYPE B EXTENDS A ( b STRING )
            |CREATE ELEMENT TYPE C EXTENDS A ( c STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  D EXTENDS B, C ( d INTEGER ),
            |  E ( e FLOAT ),
            |  (A),
            |  (B),
            |  (C),
            |  (D),
            |  (A, E),
            |  (D, E)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("a" -> CTString)
          .withNodePropertyKeys("A", "B")("a" -> CTString, "b" -> CTString)
          .withNodePropertyKeys("A", "C")("a" -> CTString, "c" -> CTString)
          .withNodePropertyKeys("A", "B", "C", "D")("a" -> CTString, "b" -> CTString, "c" -> CTString, "d" -> CTInteger)
          .withNodePropertyKeys("A", "E")("a" -> CTString, "e" -> CTFloat)
          .withNodePropertyKeys("A", "B", "C", "D", "E")("a" -> CTString, "b" -> CTString, "c" -> CTString, "d" -> CTInteger, "e" -> CTFloat)
      )
    }

    it("merges identical property keys with same type") {
      // Given
      val ddl =
        s"""|CREATE ELEMENT TYPE A ( foo STRING )
            |CREATE ELEMENT TYPE B ( foo STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (A),
            |  (A, B)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      GraphDdl(ddl).graphs(graphName).graphType.asOkapiSchema should equal(
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodePropertyKeys("A", "B")("foo" -> CTString)
      )
    }

    it("parses correct schema") {
      val ddlDefinition: DdlDefinition = parseDdl(
        s"""|SET SCHEMA foo.bar;
            |
            |CREATE ELEMENT TYPE A ( name STRING )
            |
            |CREATE ELEMENT TYPE B ( sequence INTEGER, nationality STRING?, age INTEGER? )
            |
            |CREATE ELEMENT TYPE TYPE_1
            |
            |CREATE ELEMENT TYPE TYPE_2 ( prop BOOLEAN? )
            |
            |CREATE GRAPH TYPE $typeName (
            |  A ( foo INTEGER ),
            |  C,
            |
            |  -- nodes
            |  (A),
            |  (B),
            |  (A, B),
            |  (C),
            |
            |
            |  -- edges
            |  (A)-[TYPE_1]->(B),
            |  (A, B)-[TYPE_2]->(C)
            |)
            |CREATE GRAPH $graphName OF $typeName (
            |
            |  (A) FROM foo,
            |
            |  (A)-[TYPE_1]->(B) FROM baz edge
            |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
            |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
            |)
            |""".stripMargin)
      ddlDefinition should equalWithTracing(
        DdlDefinition(List(
          SetSchemaDefinition("foo", "bar"),
          ElementTypeDefinition("A", properties = Map("name" -> CTString)),
          ElementTypeDefinition("B", properties = Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
          ElementTypeDefinition("TYPE_1"),
          ElementTypeDefinition("TYPE_2", properties = Map("prop" -> CTBoolean.nullable)),
          GraphTypeDefinition(
            name = typeName,
            statements = List(
              ElementTypeDefinition("A", properties = Map("foo" -> CTInteger)),
              ElementTypeDefinition("C"),
              NodeTypeDefinition("A"),
              NodeTypeDefinition("B"),
              NodeTypeDefinition("A", "B"),
              NodeTypeDefinition("C"),
              RelationshipTypeDefinition("A", "TYPE_1", "B"),
              RelationshipTypeDefinition("A", "B")("TYPE_2")("C")
            )),
          GraphDefinition(
            name = graphName.value,
            maybeGraphTypeName = Some(typeName),
            statements = List(
              NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("foo")))),
              RelationshipMappingDefinition(
                RelationshipTypeDefinition("A", "TYPE_1", "B"),
                List(RelationshipTypeToViewDefinition(
                  viewDef = ViewDefinition(List("baz"), "edge"),
                  startNodeTypeToView = NodeTypeToViewDefinition(
                    NodeTypeDefinition("A"),
                    ViewDefinition(List("foo"), "alias_foo"),
                    JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
                  endNodeTypeToView = NodeTypeToViewDefinition(
                    NodeTypeDefinition("B"),
                    ViewDefinition(List("bar"), "alias_bar"),
                    JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
                )))))
        ))
      )
      GraphDdl(ddlDefinition).graphs(graphName).graphType.asOkapiSchema shouldEqual Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys(Set("C"))
        .withRelationshipType("TYPE_1")
        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTBoolean.nullable)
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "TYPE_2", Set("C")))
    }

    it("creates implicit node/edge types from mappings") {
      val ddlDefinition: DdlDefinition = parseDdl(
        s"""|SET SCHEMA a.b;
            |
            |CREATE GRAPH $graphName (
            |  -- element types
            |  A ( foo INTEGER ),
            |  B,
            |  TYPE_1,
            |
            |  -- node types with mappings
            |  (A) FROM foo,
            |  (B) FROM baz,
            |  (A, B) FROM bar,
            |
            |  -- edge types with mappings
            |  (A)-[TYPE_1]->(B) FROM baz edge
            |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
            |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
            |)
            |""".stripMargin
      )
      GraphDdl(ddlDefinition).graphs(graphName).graphType.asOkapiSchema shouldEqual Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")()
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger)
        .withRelationshipType("TYPE_1")
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    }

    it("resolves element types from parent graph type") {
      val ddlDefinition: DdlDefinition = parseDdl(
        s"""|SET SCHEMA a.b;
            |
            |CREATE GRAPH TYPE $typeName (
            |  -- element types
            |  A ( foo INTEGER ),
            |  B,
            |  TYPE_1
            |)
            |
            |CREATE GRAPH $graphName OF $typeName (
            |  -- node types with mappings
            |  (A) FROM foo,
            |  (B) FROM baz,
            |  (A, B) FROM bar,
            |
            |  -- edge types with mappings
            |  (A)-[TYPE_1]->(B) FROM baz edge
            |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
            |    END NODES   (B) FROM baz alias_baz JOIN ON alias_baz.COLUMN_A = edge.COLUMN_A
            |)
            |""".stripMargin
      )
      GraphDdl(ddlDefinition).graphs(graphName).graphType.asOkapiSchema shouldEqual Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")()
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger)
        .withRelationshipType("TYPE_1")
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    }

    it("resolves shadowed element types") {
      val ddlDefinition: DdlDefinition = parseDdl(
        s"""|SET SCHEMA a.b;
            |
            |CREATE GRAPH TYPE $typeName (
            |  -- element types
            |  A ( foo INTEGER ),
            |  B,
            |  TYPE_1
            |)
            |
            |CREATE GRAPH $graphName OF $typeName (
            |  -- element types
            |  A ( bar STRING ),
            |  -- node types with mappings
            |  (A) FROM foo,
            |  (B) FROM baz,
            |  (A, B) FROM bar,
            |
            |  -- edge types with mappings
            |  (A)-[TYPE_1]->(B) FROM baz edge
            |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
            |    END NODES   (B) FROM bar alias_baz JOIN ON alias_baz.COLUMN_A = edge.COLUMN_A
            |)
            |""".stripMargin
      )
      GraphDdl(ddlDefinition).graphs(graphName).graphType.asOkapiSchema shouldEqual Schema.empty
        .withNodePropertyKeys("A")("bar" -> CTString)
        .withNodePropertyKeys("B")()
        .withNodePropertyKeys("A", "B")("bar" -> CTString)
        .withRelationshipType("TYPE_1")
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    }

    it("resolves most local element type") {
      val ddlDefinition: DdlDefinition = parseDdl(
        s"""|SET SCHEMA a.b;
            |
            |CREATE ELEMENT TYPE X (a STRING)
            |
            |CREATE GRAPH TYPE foo (
            |  X (b STRING),
            |  (X)
            |)
            |
            |CREATE GRAPH $graphName OF foo (
            |  X (c STRING),
            |  (X) FROM x -- should be (c STRING)
            |)
            |""".stripMargin
      )
      GraphDdl(ddlDefinition).graphs(graphName).graphType.asOkapiSchema shouldEqual Schema.empty
        .withNodePropertyKeys("X")("c" -> CTString)
    }

  }

  describe("OKAPI schema to GraphType") {
    it("converts single node type") {
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString)
        .asGraphType shouldEqual GraphType.empty
          .withElementType("A", "foo" -> CTString)
          .withNodeType("A")
    }

    it("converts multiple node types") {
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString)
        .withNodePropertyKeys("B")("bar" -> CTInteger)
        .asGraphType shouldEqual GraphType.empty
        .withElementType("A", "foo" -> CTString)
        .withElementType("B", "bar" -> CTInteger)
        .withNodeType("A")
        .withNodeType("B")
    }

    it("converts single node type with multiple labels") {
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTString)
        .asGraphType shouldEqual GraphType.empty
        .withElementType("A", "foo" -> CTString)
        .withElementType("B", Set("A"))
        .withNodeType("A", "B")
    }

    it("converts multiple node types with overlapping labels") {
      Schema.empty
        .withNodePropertyKeys("A")("a" -> CTString)
        .withNodePropertyKeys("A", "B")("a" -> CTString, "b" -> CTInteger)
        .withNodePropertyKeys("A", "B", "C")("a" -> CTString, "b" -> CTInteger, "c" -> CTFloat)
        .withNodePropertyKeys("A", "B", "D")("a" -> CTString, "b" -> CTInteger, "d" -> CTFloat)
        .withNodePropertyKeys("A", "B", "C", "D", "E")("a" -> CTString, "b" -> CTInteger, "c" -> CTFloat, "d" -> CTFloat, "e" -> CTBoolean)
        .asGraphType shouldEqual GraphType.empty
        .withElementType("A", "a" -> CTString)
        .withElementType("B", Set("A"), "b" -> CTInteger)
        .withElementType("C", Set("B"), "c" -> CTFloat)
        .withElementType("D", Set("B"), "d" -> CTFloat)
        .withElementType("E", Set("C", "D"), "e" -> CTBoolean)
        .withNodeType("A")
        .withNodeType("A", "B")
        .withNodeType("A", "B", "C")
        .withNodeType("A", "B", "D")
        .withNodeType("A", "B", "C", "D", "E")
    }
  }
}
