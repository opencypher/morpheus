package org.opencypher.graphddl

import org.opencypher.graphddl.GraphDdlParser.parseDdl
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import GraphDdlOps._

class GraphDdlOpsTest extends BaseTestSuite {

  val typeName = "myType"
  val graphName = GraphName("myGraph")

  describe("DDL to OKAPI schema") {
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

}
