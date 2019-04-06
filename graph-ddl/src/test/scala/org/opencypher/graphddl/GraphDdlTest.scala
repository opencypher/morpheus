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
package org.opencypher.graphddl

import org.junit.runner.RunWith
import org.opencypher.graphddl.GraphDdlParser.parseDdl
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.{FunSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class GraphDdlTest extends FunSpec with Matchers {

  val ddlString: String =
    s"""
       |SET SCHEMA dataSourceName.fooDatabaseName
       |
       |CREATE GRAPH TYPE fooSchema (
       | Person ( name STRING, age INTEGER ),
       | Book   ( title STRING ) ,
       | READS  ( rating FLOAT ) ,
       | (Person),
       | (Book),
       | (Person)-[READS]->(Book)
       |)
       |CREATE GRAPH fooGraph OF fooSchema (
       |  (Person) FROM personView1 ( person_name1 AS name )
       |           FROM personView2 ( person_name2 AS name ),
       |  (Book)   FROM bookView    ( book_title AS title ),
       |
       |  (Person)-[READS]->(Book)
       |    FROM readsView1 e ( value1 AS rating )
       |      START NODES (Person) FROM personView1 p JOIN ON p.person_id1 = e.person
       |      END   NODES (Book)   FROM bookView    b JOIN ON e.book       = b.book_id
       |    FROM readsView2 e ( value2 AS rating )
       |      START NODES (Person) FROM personView2 p JOIN ON p.person_id2 = e.person
       |      END   NODES (Book)   FROM bookView    b JOIN ON e.book       = b.book_id
       |)
     """.stripMargin

  describe("GraphDdl") {

    it("converts to GraphDDL IR") {
      val graphDdl = GraphDdl(ddlString)

      val maybeSetSchema = Some(SetSchemaDefinition("dataSourceName", "fooDatabaseName"))

      val personKey1 = NodeViewKey(NodeType("Person"), ViewId(maybeSetSchema, List("personView1")))
      val personKey2 = NodeViewKey(NodeType("Person"), ViewId(maybeSetSchema, List("personView2")))
      val bookKey = NodeViewKey(NodeType("Book"), ViewId(maybeSetSchema, List("bookView")))
      val readsKey1 = EdgeViewKey(RelationshipType("Person", "READS", "Book"), ViewId(maybeSetSchema, List("readsView1")))
      val readsKey2 = EdgeViewKey(RelationshipType("Person", "READS", "Book"), ViewId(maybeSetSchema, List("readsView2")))

      val expected = GraphDdl(
        Map(
          GraphName("fooGraph") -> Graph(GraphName("fooGraph"),
            GraphType.empty
              .withName("fooSchema")
              .withElementType(ElementType("Person", Set.empty, Map("name" -> CTString, "age" -> CTInteger)))
              .withElementType(ElementType("Book", Set.empty, Map("title" -> CTString)))
              .withElementType(ElementType("READS", Set.empty, Map("rating" -> CTFloat)))
              .withNodeType(NodeType("Person"))
              .withNodeType(NodeType("Book"))
              .withRelationshipType(RelationshipType("Person", "READS", "Book")),
            Map(
              personKey1 -> NodeToViewMapping(
                nodeType = NodeType("Person"),
                view = personKey1.viewId,
                propertyMappings = Map("name" -> "person_name1", "age" -> "age")),
              personKey2 -> NodeToViewMapping(
                nodeType = NodeType("Person"),
                view = personKey2.viewId,
                propertyMappings = Map("name" -> "person_name2", "age" -> "age")),
              bookKey -> NodeToViewMapping(
                nodeType = NodeType("Book"),
                view = bookKey.viewId,
                propertyMappings = Map("title" -> "book_title"))
            ),
            List(
              EdgeToViewMapping(
                relType = RelationshipType("Person", "READS", "Book"),
                view = readsKey1.viewId,
                startNode = StartNode(personKey1, List(Join("person_id1", "person"))),
                endNode = EndNode(bookKey, List(Join("book_id", "book"))),
                propertyMappings = Map("rating" -> "value1")),
              EdgeToViewMapping(
                relType = RelationshipType("Person", "READS", "Book"),
                view = readsKey2.viewId,
                startNode = StartNode(personKey2, List(Join("person_id2", "person"))),
                endNode = EndNode(bookKey, List(Join("book_id", "book"))),
                propertyMappings = Map("rating" -> "value2"))
            )
          )
        )
      )

      graphDdl shouldEqual expected
    }

    it("allows compact inline graph definition") {
      val ddl = GraphDdl(
        """SET SCHEMA ds1.db1
          |CREATE GRAPH myGraph (
          |  A (x STRING), B (y STRING),
          |  (A) FROM a,
          |  (A)-[B]->(A) FROM b e
          |    START NODES (A) FROM a n JOIN ON e.id = n.id
          |    END   NODES (A) FROM a n JOIN ON e.id = n.id
          |)
        """.stripMargin)

      val A_a = NodeViewKey(NodeType("A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a")))

      ddl.graphs(GraphName("myGraph")) shouldEqual Graph(
        name = GraphName("myGraph"),
        graphType = GraphType.empty
          .withName("myGraph")
          .withElementType(ElementType("A", Set.empty, Map("x" -> CTString)))
          .withElementType(ElementType("B", Set.empty, Map("y" -> CTString)))
          .withNodeType(NodeType("A"))
          .withRelationshipType(RelationshipType("A", "B", "A")),
        nodeToViewMappings = Map(
          A_a -> NodeToViewMapping(NodeType("A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a")), Map("x" -> "x"))
        ),
        edgeToViewMappings = List(
          EdgeToViewMapping(RelationshipType("A", "B", "A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("b")),
            StartNode(A_a, List(Join("id", "id"))),
            EndNode(A_a, List(Join("id", "id"))),
            Map("y" -> "y")
          )
        )
      )
    }

    it("allows these equivalent graph definitions") {
      val ddls = List(
        // most compact form
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH myGraph (
            |  A (x STRING), B (y STRING),
            |  (A) FROM a,
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id
            |)
          """.stripMargin),
        // mixed order
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH myGraph (
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id,
            |  A (x STRING), B (y STRING),
            |  (A) FROM a
            |)
          """.stripMargin),
        // explicit node and rel type definition
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH myGraph (
            |  A (x STRING), B (y STRING),
            |  (A), (A)-[B]->(A),
            |  (A) FROM a,
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id
            |)
          """.stripMargin),
        // pure type definitions extracted to graph type
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH TYPE myType (
            |  A (x STRING), B (y STRING),
            |  (A), (A)-[B]->(A)
            |)
            |CREATE GRAPH myGraph OF myType (
            |  (A) FROM a,
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id
            |)
          """.stripMargin),
        // shadowing
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH TYPE myType (
            |  A (x STRING), B (foo STRING),
            |  (A), (A)-[B]->(A)
            |)
            |CREATE GRAPH myGraph OF myType (
            |  B (y STRING),
            |  (A) FROM a,
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id
            |)
          """.stripMargin),
        // only label types in graph type
        GraphDdl(
          """SET SCHEMA ds1.db1
            |CREATE GRAPH TYPE myType (
            |  A (x STRING), B (foo STRING)
            |)
            |CREATE GRAPH myGraph OF myType (
            |  B (y STRING),
            |  (A) FROM a,
            |  (A)-[B]->(A) FROM b e
            |    START NODES (A) FROM a n JOIN ON e.id = n.id
            |    END   NODES (A) FROM a n JOIN ON e.id = n.id
            |)
          """.stripMargin)
      )
      // implicit graph type
      ddls(1) shouldEqual ddls.head
      ddls(2) shouldEqual ddls.head

      // explicit graph type
      ddls(4) shouldEqual ddls(3)
      ddls(5) shouldEqual ddls(3)
    }

    it("allows these equivalent graph type definitions") {
      val ddls = List(
        // explicit node and rel type definitions
        GraphDdl(
          """CREATE GRAPH TYPE myType (
            |  A (x STRING), B (y STRING), C (z STRING),
            |  (A), (C),
            |  (A)-[B]->(C)
            |)
            |CREATE GRAPH myGraph OF myType ()
          """.stripMargin),
        // shadowing
        GraphDdl(
          """CREATE ELEMENT TYPE A (foo STRING)
            |CREATE GRAPH TYPE myType (
            |  A (x STRING), B (y STRING), C (z STRING),
            |  (A), (C),
            |  (A)-[B]->(C)
            |)
            |CREATE GRAPH myGraph OF myType ()
          """.stripMargin)
      )

      ddls(1) shouldEqual ddls.head
    }
  }

  describe("GraphType") {

    val typeName = "myType"
    val graphName = GraphName("myGraph")

    it("can construct schema with node label") {

      val ddl =
        s"""|CREATE ELEMENT TYPE A ( name STRING )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (A)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
          """.stripMargin

      val expected = GraphType(typeName)
        .withElementType("A", "name" -> CTString)
        .withNodeType("A")

      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("name" -> CTString)
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

      val expected = GraphType(typeName)
        .withElementType("A", "name" -> CTString)
        .withNodeType("A")
        .withRelationshipType("A", "A", "A")

      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("name" -> CTString)
      expected.relationshipPropertyKeys("A", "A", "A") shouldEqual Map("name" -> CTString)
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

      val expected = GraphType(typeName)
        .withElementType("Node1", "val" -> CTString)
        .withElementType("Node2", "val" -> CTString)
        .withElementType("REL", "name" -> CTString)
        .withNodeType("Node1")
        .withNodeType("Node2")
        .withRelationshipType("Node1", "REL", "Node2")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("Node1") shouldEqual Map("val" -> CTString)
      expected.nodePropertyKeys("Node2") shouldEqual Map("val" -> CTString)
      expected.relationshipPropertyKeys("Node1", "REL", "Node2") shouldEqual Map("name" -> CTString)
    }

    it("can construct schema with inherited node and edge labels") {
      val ddl =
        s"""|CREATE ELEMENT TYPE Node1 ( foo STRING )
            |CREATE ELEMENT TYPE Node2 EXTENDS Node1 ( bar INTEGER )
            |CREATE ELEMENT TYPE Node3 EXTENDS Node2 ( baz BOOLEAN )
            |CREATE ELEMENT TYPE REL1 ( name STRING )
            |CREATE ELEMENT TYPE REL2 EXTENDS REL1 ( since INTEGER )
            |CREATE ELEMENT TYPE REL3 EXTENDS REL2 ( age BOOLEAN )
            |
            |CREATE GRAPH TYPE $typeName (
            |  (Node1),
            |  (Node2),
            |  (Node3),
            |  (Node1)-[REL1]->(Node1),
            |  (Node1)-[REL2]->(Node2),
            |  (Node2)-[REL3]->(Node2)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      val expected = GraphType(typeName)
        .withElementType("Node1", "foo" -> CTString)
        .withElementType("Node2", parents = Set("Node1"), "bar" -> CTInteger)
        .withElementType("Node3", parents = Set("Node2"), "baz" -> CTBoolean)
        .withElementType("REL1", "name" -> CTString)
        .withElementType("REL2", parents = Set("REL1"), "since" -> CTInteger)
        .withElementType("REL3", parents = Set("REL2"), "age" -> CTBoolean)
        .withNodeType("Node1")
        .withNodeType("Node2")
        .withNodeType("Node3")
        .withRelationshipType("Node1", "REL1", "Node1")
        .withRelationshipType("Node1", "REL2", "Node2")
        .withRelationshipType("Node2", "REL3", "Node2")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("Node1") shouldEqual Map("foo" -> CTString)
      expected.nodePropertyKeys("Node1", "Node2") shouldEqual Map("foo" -> CTString, "bar" -> CTInteger)
      expected.nodePropertyKeys("Node1", "Node2", "Node3") shouldEqual Map("foo" -> CTString, "bar" -> CTInteger, "baz" -> CTBoolean)
      expected.relationshipPropertyKeys(Set("Node1"), Set("REL1"), Set("Node1")) shouldEqual Map("name" -> CTString)
      expected.relationshipPropertyKeys(Set("Node1"), Set("REL1", "REL2"), Set("Node1", "Node2")) shouldEqual Map("name" -> CTString, "since" -> CTInteger)
      expected.relationshipPropertyKeys(Set("Node1", "Node2"), Set("REL1", "REL2", "REL3"), Set("Node1", "Node2")) shouldEqual Map("name" -> CTString, "since" -> CTInteger, "age" -> CTBoolean)
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

      val expected = GraphType(typeName)
        .withElementType("Node", "foo" -> CTInteger)
        .withNodeType("Node")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("Node") shouldEqual Map("foo" -> CTInteger)

    }

    it("can construct schema with node labels with element key") {
      val ddl =
        s"""|CREATE ELEMENT TYPE Node () KEY akey (val)
            |
            |CREATE GRAPH TYPE $typeName (
            |  (Node)
            |)
            |CREATE GRAPH $graphName OF $typeName ()
            |""".stripMargin

      val expected = GraphType(typeName)
        .withElementType(ElementType(
          name = "Node",
          maybeKey = Some("akey" -> Set("val"))))
        .withNodeType("Node")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
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

      val expected = GraphType(typeName)
        .withElementType("MyLabel", "property" -> CTString, "data" -> CTInteger.nullable)
        .withElementType("LocalLabel1", "property" -> CTString)
        .withElementType("LocalLabel2")
        .withElementType("REL_TYPE1", "property" -> CTBoolean)
        .withElementType("REL_TYPE2")
        .withNodeType("MyLabel")
        .withNodeType("LocalLabel1")
        .withNodeType("LocalLabel1", "LocalLabel2")
        .withRelationshipType(Set("MyLabel"), Set("REL_TYPE1"), Set("LocalLabel1"))
        .withRelationshipType(Set("LocalLabel1", "LocalLabel2"), Set("REL_TYPE2"), Set("MyLabel"))
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("MyLabel") shouldEqual Map("property" -> CTString, "data" -> CTInteger.nullable)
      expected.nodePropertyKeys("LocalLabel1") shouldEqual Map("property" -> CTString)
      expected.nodePropertyKeys("LocalLabel2") shouldEqual Map.empty
      expected.nodePropertyKeys("LocalLabel1", "LocalLabel2") shouldEqual Map("property" -> CTString)
      expected.relationshipPropertyKeys(Set("MyLabel"), Set("REL_TYPE1"), Set("LocalLabel1")) shouldEqual Map("property" -> CTBoolean)
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

      val expected = GraphType(typeName)
        .withElementType("A", "foo" -> CTString)
        .withElementType("B", "bar" -> CTString)
        .withNodeType("A")
        .withNodeType("A", "B")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTString)
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTString, "bar" -> CTString)
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

      val expected = GraphType(typeName)
        .withElementType("A", "foo" -> CTString)
        .withElementType(ElementType(name = "B", parents = Set("A"), properties = Map("bar" -> CTString)))
        .withNodeType("A")
        .withNodeType("A", "B")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTString)
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTString, "bar" -> CTString)
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

      val expected = GraphType(typeName)
        .withElementType("A", "a" -> CTString)
        .withElementType(ElementType(name = "B", parents = Set("A"), properties = Map("b" -> CTString)))
        .withElementType(ElementType(name = "C", parents = Set("A"), properties = Map("c" -> CTString)))
        .withElementType(ElementType(name = "D", parents = Set("B", "C"), properties = Map("d" -> CTInteger)))
        .withElementType("E", "e" -> CTFloat)
        .withNodeType("A")
        .withNodeType("A", "B")
        .withNodeType("A", "C")
        .withNodeType("A", "B", "C", "D")
        .withNodeType("A", "E")
        .withNodeType("A", "B", "C", "D", "E")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("a" -> CTString)
      expected.nodePropertyKeys("A", "B") shouldEqual Map("a" -> CTString, "b" -> CTString)
      expected.nodePropertyKeys("A", "C") shouldEqual Map("a" -> CTString, "c" -> CTString)
      expected.nodePropertyKeys("A", "B", "C", "D") shouldEqual Map("a" -> CTString, "b" -> CTString, "c" -> CTString, "d" -> CTInteger)
      expected.nodePropertyKeys("A", "E") shouldEqual Map("a" -> CTString, "e" -> CTFloat)
      expected.nodePropertyKeys("A", "B", "C", "D", "E") shouldEqual Map("a" -> CTString, "b" -> CTString, "c" -> CTString, "d" -> CTInteger, "e" -> CTFloat)
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

      val expected = GraphType(typeName)
        .withElementType("A", "foo" -> CTString)
        .withElementType("B", "foo" -> CTString)
        .withNodeType("A")
        .withNodeType("A", "B")
      GraphDdl(ddl).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTString)
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTString)
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
      val expected = GraphType(typeName)
        .withElementType("A", "foo" -> CTInteger)
        .withElementType("B", "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withElementType("C")
        .withElementType("TYPE_1")
        .withElementType("TYPE_2", "prop" -> CTBoolean.nullable)
        .withNodeType("A")
        .withNodeType("B")
        .withNodeType("A", "B")
        .withNodeType("C")
        .withRelationshipType("A", "TYPE_1", "B")
        .withRelationshipType(Set("A", "B"), Set("TYPE_2"), Set("C"))
      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTInteger)
      expected.nodePropertyKeys("B") shouldEqual Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTInteger, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
      expected.nodePropertyKeys("C") shouldEqual Map.empty
      expected.relationshipPropertyKeys("A", "TYPE_1", "B") shouldEqual Map.empty
      expected.relationshipPropertyKeys(Set("A", "B"), Set("TYPE_2"), Set("C")) shouldEqual Map("prop" -> CTBoolean.nullable)
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
      val expected = GraphType(graphName.value)
        .withElementType("A", "foo" -> CTInteger)
        .withElementType("B")
        .withElementType("TYPE_1")
        .withNodeType("A")
        .withNodeType("B")
        .withNodeType("A", "B")
        .withRelationshipType("A", "TYPE_1", "B")
      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTInteger)
      expected.nodePropertyKeys("B") shouldEqual Map.empty
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTInteger)
      expected.relationshipPropertyKeys("A", "TYPE_1", "B") shouldEqual Map.empty
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
      val expected = GraphType(typeName)
        .withElementType("A", "foo" -> CTInteger)
        .withElementType("B")
        .withElementType("TYPE_1")
        .withNodeType("A")
        .withNodeType("B")
        .withNodeType("A", "B")
        .withRelationshipType("A", "TYPE_1", "B")
      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("foo" -> CTInteger)
      expected.nodePropertyKeys("B") shouldEqual Map.empty
      expected.nodePropertyKeys("A", "B") shouldEqual Map("foo" -> CTInteger)
      expected.relationshipPropertyKeys("A", "TYPE_1", "B") shouldEqual Map.empty
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
      val expected = GraphType(typeName)
        .withElementType("A", "bar" -> CTString)
        .withElementType("B")
        .withElementType("TYPE_1")
        .withNodeType("A")
        .withNodeType("B")
        .withNodeType("A", "B")
        .withRelationshipType("A", "TYPE_1", "B")
      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("A") shouldEqual Map("bar" -> CTString)
      expected.nodePropertyKeys("B") shouldEqual Map.empty
      expected.nodePropertyKeys("A", "B") shouldEqual Map("bar" -> CTString)
      expected.relationshipPropertyKeys("A", "TYPE_1", "B") shouldEqual Map.empty
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
      val expected = GraphType("foo")
        .withElementType("X", "c" -> CTString)
        .withNodeType("X")
      GraphDdl(ddlDefinition).graphs(graphName).graphType shouldEqual expected
      expected.nodePropertyKeys("X") shouldEqual Map("c" -> CTString)
    }
  }

  describe("Join key extraction") {

    it("extracts join keys for a given node view key in start node position") {
      val maybeJoinColumns = GraphDdl(ddlString).graphs(GraphName("fooGraph"))
        .nodeIdColumnsFor(NodeViewKey(
          NodeType("Person"),
          ViewId(Some(SetSchemaDefinition("dataSourceName", "fooDatabaseName")), List("personView1"))))

      maybeJoinColumns shouldEqual Some(List("person_id1"))
    }

    it("extracts join keys for a given node view key in end node position") {
      val maybeJoinColumns = GraphDdl(ddlString).graphs(GraphName("fooGraph"))
        .nodeIdColumnsFor(NodeViewKey(
          NodeType("Book"),
          ViewId(Some(SetSchemaDefinition("dataSourceName", "fooDatabaseName")), List("bookView"))))

      maybeJoinColumns shouldEqual Some(List("book_id"))
    }

    it("does not extract join keys for an invalid node view key") {
      val maybeJoinColumns = GraphDdl(ddlString).graphs(GraphName("fooGraph"))
        .nodeIdColumnsFor(NodeViewKey(NodeType("A"), ViewId(None, List("dataSourceName", "fooDatabaseName", "A"))))

      maybeJoinColumns shouldEqual None
    }
  }

  describe("SET SCHEMA") {

    it("allows SET SCHEMA and fully qualified names") {
      val ddl = GraphDdl(
        """SET SCHEMA ds1.db1
          |
          |CREATE GRAPH TYPE fooSchema (
          |  Person,
          |  Account,
          |  (Person),
          |  (Account)
          |)
          |CREATE GRAPH fooGraph OF fooSchema (
          |  (Person)  FROM personView,
          |  (Account) FROM ds2.db2.accountView
          |)
        """.stripMargin)

      ddl.graphs(GraphName("fooGraph")).nodeToViewMappings.keys shouldEqual Set(
        NodeViewKey(NodeType("Person"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("personView"))),
        NodeViewKey(NodeType("Account"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("ds2", "db2", "accountView")))
      )
    }
  }

  describe("validate EXTENDS syntax for mappings") {

    val A_a = NodeViewKey(NodeType("A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a")))
    val A_ab = NodeViewKey(NodeType("A", "B"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a_b")))

    it("allows compact inline graph definition with complex node type") {
      val ddl = GraphDdl(
        """SET SCHEMA ds1.db1
          |CREATE GRAPH myGraph (
          |  A (x STRING),
          |  B (y STRING),
          |  R (y STRING),
          |
          |  (A)-[R]->(A),
          |  (A, B)-[R]->(A),
          |
          |  (A) FROM a,
          |  (A, B) FROM a_b,
          |
          |  (A)-[R]->(A) FROM r e
          |    START NODES (A) FROM a n JOIN ON e.id = n.id
          |    END   NODES (A) FROM a n JOIN ON e.id = n.id,
          |  (A, B)-[R]->(A) FROM r e
          |    START NODES (A, B) FROM a_b n JOIN ON e.id = n.id
          |    END   NODES (A)    FROM a   n JOIN ON e.id = n.id
          |)
        """.stripMargin)

      ddl.graphs(GraphName("myGraph")) shouldEqual Graph(
        name = GraphName("myGraph"),
        graphType = GraphType.empty
          .withName("myGraph")
          .withElementType(ElementType("A", Set.empty, Map("x" -> CTString)))
          .withElementType(ElementType("B", Set.empty, Map("y" -> CTString)))
          .withElementType(ElementType("R", Set.empty, Map("y" -> CTString)))
          .withNodeType(NodeType("A"))
          .withNodeType(NodeType("A", "B"))
          .withRelationshipType(RelationshipType("A", "R", "A"))
          .withRelationshipType(RelationshipType(NodeType("A", "B"), Set("R"), NodeType("A"))),
        nodeToViewMappings = Map(
          A_a -> NodeToViewMapping(NodeType("A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a")), Map("x" -> "x")),
          A_ab -> NodeToViewMapping(NodeType("A", "B"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a_b")), Map("x" -> "x", "y" -> "y"))
        ),
        edgeToViewMappings = List(
          EdgeToViewMapping(RelationshipType("A", "R", "A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("r")),
            StartNode(A_a, List(Join("id", "id"))),
            EndNode(A_a, List(Join("id", "id"))),
            Map("y" -> "y")
          ),
          EdgeToViewMapping(RelationshipType(NodeType("A", "B"), Set("R"), NodeType("A")), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("r")),
            StartNode(A_ab, List(Join("id", "id"))),
            EndNode(A_a, List(Join("id", "id"))),
            Map("y" -> "y")
          )
        )
      )
    }

    it("allows compact inline graph definition with complex node type based on inheritance") {
      val ddl = GraphDdl(
        """SET SCHEMA ds1.db1
          |CREATE GRAPH myGraph (
          |  A (x STRING),
          |  B EXTENDS A (y STRING),
          |  R (y STRING),
          |
          |  (A)-[R]->(A),
          |  (B)-[R]->(A),
          |
          |  (A) FROM a,
          |  (B) FROM a_b,
          |
          |  (A)-[R]->(A) FROM r e
          |    START NODES (A) FROM a n JOIN ON e.id = n.id
          |    END   NODES (A) FROM a n JOIN ON e.id = n.id,
          |  (B)-[R]->(A) FROM r e
          |    START NODES (B) FROM a_b n JOIN ON e.id = n.id
          |    END   NODES (A) FROM a   n JOIN ON e.id = n.id
          |)
        """.stripMargin)

      ddl.graphs(GraphName("myGraph")) shouldEqual Graph(
        name = GraphName("myGraph"),
        graphType = GraphType("myGraph")
          .withElementType(ElementType("A", Set.empty, Map("x" -> CTString)))
          .withElementType(ElementType("B", Set("A"), Map("y" -> CTString)))
          .withElementType(ElementType("R", Set.empty, Map("y" -> CTString)))
          .withNodeType(NodeType("A"))
          .withNodeType(NodeType("A", "B"))
          .withRelationshipType(RelationshipType("A", "R", "A"))
          .withRelationshipType(RelationshipType(NodeType("A", "B"), Set("R"), NodeType("A"))),
        nodeToViewMappings = Map(
          A_a -> NodeToViewMapping(NodeType("A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a")), Map("x" -> "x")),
          A_ab -> NodeToViewMapping(NodeType("A", "B"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("a_b")), Map("x" -> "x", "y" -> "y"))
        ),
        edgeToViewMappings = List(
          EdgeToViewMapping(RelationshipType("A", "R", "A"), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("r")),
            StartNode(A_a, List(Join("id", "id"))),
            EndNode(A_a, List(Join("id", "id"))),
            Map("y" -> "y")
          ),
          EdgeToViewMapping(RelationshipType(NodeType("A", "B"), Set("R"), NodeType("A")), ViewId(Some(SetSchemaDefinition("ds1", "db1")), List("r")),
            StartNode(A_ab, List(Join("id", "id"))),
            EndNode(A_a, List(Join("id", "id"))),
            Map("y" -> "y")
          )
        )
      )
    }
  }

  describe("failure handling") {

    it("fails on duplicate node types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          |  A (),
          |  B (),
          |  (A, B),
          |  (A, B)
          |)
        """.stripMargin
      )
      e.getFullMessage should (include("fooSchema") and include("node type") and include("(A,B)"))
    }

    it("fails on duplicate anonymous node types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          |  A (),
          |  X (),
          |  B EXTENDS A (),
          |  (A, B, X),
          |  (A, X),
          |  (B, X)
          |)
        """.stripMargin
      )
      e.getFullMessage should (include("fooSchema") and include("node type") and include("(A,B,X)"))
    }

    it("fails on duplicate relationship types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          |  A (),
          |  B (),
          |  (A)-[B]->(A),
          |  (A)-[B]->(A)
          |)
        """.stripMargin
      )
      e.getFullMessage should (include("fooSchema") and include("relationship type") and include("(A)-[B]->(A)"))
    }

    it("fails on duplicate relationship types using anonymous node types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          |  A,
          |  B EXTENDS A (),
          |  X,
          |  FOO,
          |  (A, B)-[FOO]->(X),
          |  (B)-[FOO]->(X)
          |)
        """.stripMargin
      )
      e.getFullMessage should (include("fooSchema") and include("relationship type") and include("(A,B)-[FOO]->(X)"))
    }

    it("fails on duplicate node mappings") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA db.schema
          |
          |CREATE GRAPH TYPE fooSchema (
          | Person,
          | (Person)
          |)
          |CREATE GRAPH fooGraph OF fooSchema (
          |  (Person) FROM personView
          |           FROM personView
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("(Person)") and include("personView"))
    }

    it("fails on duplicate relationship mappings") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA db.schema
          |
          |CREATE GRAPH TYPE fooSchema (
          | Person,
          | KNOWS,
          | (Person),
          | (Person)-[KNOWS]->(Person)
          |)
          |CREATE GRAPH fooGraph OF fooSchema (
          | (Person)-[KNOWS]->(Person)
          |   FROM pkpView e
          |     START NODES (Person) FROM a n JOIN ON e.id = n.id
          |     END   NODES (Person) FROM a n JOIN ON e.id = n.id,
          |   FROM pkpView e
          |     START NODES (Person) FROM a n JOIN ON e.id = n.id
          |     END   NODES (Person) FROM a n JOIN ON e.id = n.id
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("(Person)-[KNOWS]->(Person)") and include("pkpView"))
    }

    it("fails on duplicate global labels") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE ELEMENT TYPE Person
          |CREATE ELEMENT TYPE Person
        """.stripMargin)
      e.getFullMessage should include("Person")
    }

    it("fails on duplicate local labels") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          | Person,
          | Person
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooSchema") and include("Person"))
    }

    it("fails on duplicate graph types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema ()
          |CREATE GRAPH TYPE fooSchema ()
        """.stripMargin)
      e.getFullMessage should include("fooSchema")
    }

    it("fails on duplicate graphs") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema ()
          |CREATE GRAPH fooGraph OF fooSchema ()
          |CREATE GRAPH fooGraph OF fooSchema ()
        """.stripMargin)
      e.getFullMessage should include("fooGraph")
    }

    it("fails on unresolved graph type") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema ()
          |CREATE GRAPH fooGraph OF barSchema ()
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("fooSchema") and include("barSchema"))
    }

    it("fails on unresolved labels") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          | Person1,
          | Person2,
          | (Person3, Person4)
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooSchema") and include("Person3") and include("Person4"))
    }

    it("fails on unresolved labels in mapping") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH fooGraph (
          | Person1,
          | Person2,
          | (Person3, Person4) FROM x
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("Person3") and include("Person4"))
    }

    it("fails on incompatible property types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |CREATE GRAPH TYPE fooSchema (
          | Person1 ( age STRING ) ,
          | Person2 ( age INTEGER ) ,
          | (Person1, Person2)
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooSchema") and include("Person1") and include("Person2") and include("age") and include("STRING") and include("INTEGER"))
    }

    it("fails on unresolved property names") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA a.b
          |CREATE GRAPH TYPE fooSchema (
          | Person ( age1 STRING ) ,
          | (Person)
          |)
          |CREATE GRAPH fooGraph OF fooSchema (
          |  (Person) FROM personView ( person_name AS age2 )
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("Person") and include("personView") and include("age1") and include("age2"))
    }

    it("fails on unresolved inherited element types") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA a.b
          |CREATE GRAPH TYPE fooSchema (
          | Person ( name STRING ) ,
          | Employee EXTENDS MissingPerson ( dept STRING ) ,
          | (Employee)
          |)
          |CREATE GRAPH fooGraph OF fooSchema (
          |  (Employee) FROM employeeView ( person_name AS name, emp_dept AS dept )
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooSchema") and include("Employee") and include("MissingPerson"))
    }

    it("fails on unresolved inherited element types within inlined graph type") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA a.b
          |CREATE GRAPH fooGraph (
          |  Person ( name STRING ),
          |  Employee EXTENDS MissingPerson ( dept STRING ),
          |
          |  (Employee) FROM employeeView ( person_name AS name, emp_dept AS dept )
          |)
        """.stripMargin)
      e.getFullMessage should (include("fooGraph") and include("Employee") and include("MissingPerson"))
    }

    it("fails on cyclic element type inheritance") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA a.b
          |CREATE GRAPH fooGraph (
          |  A EXTENDS B ( a STRING ),
          |  B EXTENDS A ( b STRING ),
          |
          |  (A) FROM a ( A_a AS a, B_b AS b ),
          |  (B) FROM b ( A_a AS a, B_b AS b )
          |)
        """.stripMargin)
      e.getFullMessage should (include("Circular dependency") and include("A -> B -> A"))
    }

    it("fails on conflicting property types in inheritance hierarchy") {
      val e = the[GraphDdlException] thrownBy GraphDdl(
        """
          |SET SCHEMA a.b
          |CREATE GRAPH fooGraph (
          |  A ( x STRING ),
          |  B ( x INTEGER ),
          |  C EXTENDS A, B (),
          |
          |  (C)
          |)
        """.stripMargin)
      e.getFullMessage should (include("(A,B,C)") and include("x") and include("INTEGER") and include("STRING"))
    }

    it("fails if an unknown property key is mapped to a column") {
      val ddlString =
        s"""|CREATE GRAPH TYPE myType (
            |  A ( foo STRING ),
            |  (A)
            |)
            |CREATE GRAPH myGraph OF myType (
            |  (A) FROM view_A ( column AS bar )
            |)
            |""".stripMargin

      an[GraphDdlException] shouldBe thrownBy {
        GraphDdl(parseDdl(ddlString)).graphs(GraphName("myGraph")).graphType
      }
    }

    it("fails if a node view key is referenced by different join columns") {
      val e = the[GraphDdlException] thrownBy {
        val ddlString =
          s"""|CREATE GRAPH TYPE myType (
              |  A ( foo STRING ),
              |  B ( bar STRING ),
              |  R,
              |  (A),
              |  (B),
              |  (A)-[R]->(B)
              |)
              |CREATE GRAPH myGraph OF myType (
              |  (A) FROM view_A,
              |  (B) FROM view_B,
              |  (A)-[R]->(A)
              |   FROM view_R1 e
              |     START NODES (A) FROM view_A n JOIN ON e.id = n.id1
              |     END   NODES (B) FROM view_B n JOIN ON e.id = n.id,
              |   FROM view_R2 e
              |     START NODES (A) FROM view_A n JOIN ON e.id = n.id2
              |     END   NODES (B) FROM view_B n JOIN ON e.id = n.id
              |)
              |""".stripMargin

        GraphDdl(parseDdl(ddlString)).graphs(GraphName("myGraph"))
      }
      e.getFullMessage should (include("Inconsistent join column definition") and include("(A)") and include("view_A"))
    }
  }
}
