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

import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.graphddl.GraphDdlParser._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.util.ParserUtils._
import org.opencypher.okapi.testing.{BaseTestSuite, TestNameFixture}
import org.scalatest.mockito.MockitoSugar

class GraphDdlParserTest extends BaseTestSuite with MockitoSugar with TestNameFixture {

  override val separator = "parses"

  private def success[T](
    parser: fastparse.core.Parser[T, Char, String],
    input: String,
    expectation: T

  ): Unit = success(parser, expectation, input)

  private def success[T](
    parser: fastparse.core.Parser[T, Char, String],
    expectation: T,
    input: String = testName
  ): Unit = {

    val parsed = parser.entireInput.parse(input)

    parsed match {
      case Failure(lastParser, _, extra) =>
        debug(parser, input)
      case _ =>
    }

    parsed should matchPattern {
      case Success(`expectation`, _) =>
    }
  }

  private def failure[T, Elem](parser: fastparse.core.Parser[T, Elem, String]): Unit = {
    parser.parse(testName) should matchPattern {
      case Failure(_, _, _) =>
    }
  }

  def debug[T](parser: fastparse.core.Parser[T, Char, String], input: String): T = {
    parser.parse(input) match {
      case Success(v, _) =>
        v
      case Failure(failedParser, index, extra) =>
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, extra.input.length) - index
        val locationPointer =
          s"""|\t${extra.input.slice(index - before, index + after).replace('\n', ' ')}
              |\t${"~" * before + "^" + "~" * after}
           """.stripMargin
        throw DdlParsingException(index, locationPointer, extra.traced.expected, extra.traced.stack.toList)
    }
  }


  val emptyMap = Map.empty[String, CypherType]
  val emptyList: List[Nothing] = List.empty[Nothing]
  val emptySchemaDef: GraphTypeBody = GraphTypeBody()

  describe("set schema") {
    it("parses SET SCHEMA foo.bar") {
      success(setSchemaDefinition, SetSchemaDefinition("foo", "bar"))
    }

    it("parses SET SCHEMA foo.bar;") {
      success(setSchemaDefinition, SetSchemaDefinition("foo", "bar"))
    }
  }

  describe("label definitions") {
    it("parses A") {
      success(elementTypeDefinition, ElementTypeDefinition("A"))
    }

    it("parses  A ( foo  string? )") {
      success(elementTypeDefinition, ElementTypeDefinition("A", Map("foo" -> CTString.nullable)))
    }

    it("parses  A ( key FLOAT )") {
      success(elementTypeDefinition, ElementTypeDefinition("A", Map("key" -> CTFloat)))
    }

    it("parses  A ( key FLOAT? )") {
      success(elementTypeDefinition, ElementTypeDefinition("A", Map("key" -> CTFloat.nullable)))
    }

    it("!parses  A ( key _ STRING )") {
      failure(elementTypeDefinition)
    }

    it("parses  A ( key1 FLOAT, key2 STRING)") {
      success(elementTypeDefinition, ElementTypeDefinition("A", Map("key1" -> CTFloat, "key2" -> CTString)))
    }

    it("!parses  A ()") {
      failure(elementTypeDefinition)
    }
  }

  describe("catalog label definition") {
    it("parses CREATE ELEMENT TYPE A") {
      success(globalElementTypeDefinition, ElementTypeDefinition("A"))
    }

    it("parses CREATE ELEMENT TYPE A ( foo STRING ) ") {
      success(globalElementTypeDefinition, ElementTypeDefinition("A", Map("foo" -> CTString)))
    }

    it("parses CREATE ELEMENT TYPE A KEY A_NK   (foo,   bar)") {
      success(globalElementTypeDefinition, ElementTypeDefinition("A", Map.empty, Some("A_NK" -> Set("foo", "bar"))))
    }

    it("parses CREATE ELEMENT TYPE A ( foo STRING ) KEY A_NK (foo,   bar)") {
      success(globalElementTypeDefinition, ElementTypeDefinition("A", Map("foo" -> CTString), Some("A_NK" -> Set("foo", "bar"))))
    }

    it("!parses CREATE ELEMENT TYPE A ( foo STRING ) KEY A ()") {
      failure(globalElementTypeDefinition)
    }
  }

  describe("schema pattern definitions") {

    it("parses <1>") {
      success(cardinalityConstraint, CardinalityConstraint(1, Some(1)))
    }

    it("parses <1, *>") {
      success(cardinalityConstraint, CardinalityConstraint(1, None))
    }

    it("parses <1 .. *>") {
      success(cardinalityConstraint, CardinalityConstraint(1, None))
    }

    it("parses <*>") {
      success(cardinalityConstraint, CardinalityConstraint(0, None))
    }

    it("parses <1, 3>") {
      success(cardinalityConstraint, CardinalityConstraint(1, Some(3)))
    }

    it("parses (A)-[TYPE]->(B)") {
      success(patternDefinition, PatternDefinition(sourceNodeTypes = Set(Set("A")), relTypes = Set("TYPE"), targetNodeTypes = Set(Set("B"))))
    }

    it("parses (L1 | L2) <0 .. *> - [R1 | R2] -> <1>(L3)") {
      success(patternDefinition, PatternDefinition(
        Set(Set("L1"), Set("L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L1 | L2) - [R1 | R2] -> <1>(L3)") {
      success(patternDefinition, PatternDefinition(
        Set(Set("L1"), Set("L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L1, L2) - [R1 | R2] -> <1>(L3)") {
      success(patternDefinition, PatternDefinition(
        Set(Set("L1", "L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L4 | L1, L2 | L3 , L5) - [R1 | R2] -> <1>(L3)") {
      success(patternDefinition, PatternDefinition(
        Set(Set("L4"), Set("L1", "L2"), Set("L3", "L5")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }
  }

  describe("schema definitions") {

    it("parses multiple label definitions") {
      parse(
        """|SET SCHEMA foo.bar
           |
           |CREATE ELEMENT TYPE A ( name STRING )

           |
           |CREATE ELEMENT TYPE B ( sequence INTEGER, nationality STRING?, age INTEGER? )
           |
           |CREATE ELEMENT TYPE TYPE_1
           |
           |CREATE ELEMENT TYPE TYPE_2 ( prop BOOLEAN? ) """.stripMargin) shouldEqual
        DdlDefinition(List(
          SetSchemaDefinition("foo", "bar"),
          ElementTypeDefinition("A", Map("name" -> CTString)),
          ElementTypeDefinition("B", Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
          ElementTypeDefinition("TYPE_1"),
          ElementTypeDefinition("TYPE_2", Map("prop" -> CTBoolean.nullable))
        ))
    }

    it("parses a schema with node, rel, and schema pattern definitions") {

      val input =
        """|CREATE GRAPH TYPE mySchema (
           |
           |  --NODES
           |  (A),
           |  (B),
           |  (A, B),
           |
           |  --EDGES
           |  [TYPE_1],
           |  [TYPE_2],
           |
           |  (A | B) <0 .. *> - [TYPE_1] -> <1> (B),
           |  (A) <*> - [TYPE_1] -> (A)
           |)
        """.stripMargin
      success(graphTypeDefinition, input, GraphTypeDefinition(
        name = "mySchema",
        graphTypeBody = GraphTypeBody(List(
          NodeTypeDefinition(Set("A")),
          NodeTypeDefinition(Set("B")),
          NodeTypeDefinition(Set("A", "B")),
          RelationshipTypeDefinition("TYPE_1"),
          RelationshipTypeDefinition("TYPE_2"),
          PatternDefinition(
            Set(Set("A"), Set("B")),
            CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)),
            Set(Set("B"))),
          PatternDefinition(
            Set(Set("A")),
            CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(0, None),
            Set(Set("A")))
      ))))
    }

    it("parses CREATE GRAPH TYPE mySchema ( (A)-[TYPE]->(B) )") {
      success(graphTypeDefinition,
        GraphTypeDefinition("mySchema",
          GraphTypeBody(List(
            PatternDefinition(sourceNodeTypes = Set(Set("A")), relTypes = Set("TYPE"), targetNodeTypes = Set(Set("B")))
        ))))
    }

    it("parses a schema with node, rel, and schema pattern definitions in any order") {

      val input =
        """|CREATE GRAPH TYPE mySchema (
           |  (A | B) <0 .. *> - [TYPE_1] -> <1> (B),
           |  (A),
           |  (A, B),
           |  (A) <*> - [TYPE_1] -> (A),
           |  [TYPE_1],
           |  (B),
           |  [TYPE_2]
           |)
        """.stripMargin
      success(graphTypeDefinition, input, GraphTypeDefinition(
        name = "mySchema",
        graphTypeBody = GraphTypeBody(List(
          PatternDefinition(
            Set(Set("A"), Set("B")),
            CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)),
            Set(Set("B"))),
          NodeTypeDefinition(Set("A")),
          NodeTypeDefinition(Set("A", "B")),
          PatternDefinition(
            Set(Set("A")),
            CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(0, None),
            Set(Set("A"))),
          RelationshipTypeDefinition("TYPE_1"),
          NodeTypeDefinition(Set("B")),
          RelationshipTypeDefinition("TYPE_2")
        ))))
    }
  }

  describe("graph definitions") {
    it("parses CREATE GRAPH myGraph OF foo ()") {
      success(graphDefinition, GraphDefinition("myGraph", Some("foo")))
    }

    it("parses CREATE GRAPH myGraph OF mySchema ()") {
      success(graphDefinition, GraphDefinition("myGraph", Some("mySchema")))
    }

    it("parses a graph definition with inlined schema") {
      val expectedSchemaDefinition = GraphTypeBody(List(
        ElementTypeDefinition("A", properties = Map("foo" -> CTString)),
        ElementTypeDefinition("B"),
        NodeTypeDefinition(Set("A", "B")),
        RelationshipTypeDefinition("B")
      ))
      graphDefinition.parse(
        """|CREATE GRAPH myGraph OF (
           | A ( foo STRING ) ,
           | B,
           |
           | (A,B),
           | [B]
           |) ()
        """.stripMargin) should matchPattern {
        case Success(GraphDefinition("myGraph", None, `expectedSchemaDefinition`, `emptyList`), _) =>
      }
    }
  }

  describe("Node mappings and relationship mappings") {

    it("parses (A) FROM view") {
      success(nodeMappingDefinition, NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("view")))))
    }

    it("parses (A) FROM view (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappingDefinition, NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("view"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))))
    }
    it("parses (A) FROM viewA FROM viewB") {
      success(nodeMappingDefinition, NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA")), NodeToViewDefinition(List("viewB")))))
    }

    it("parses (A) FROM viewA, (B) FROM viewB") {
      success(nodeMappings, List(NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA")))), NodeMappingDefinition(NodeTypeDefinition("B"), List(NodeToViewDefinition(List("viewB"))))))
    }

    it("parses (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappings, List(
        NodeMappingDefinition(NodeTypeDefinition("A"), List(
          NodeToViewDefinition(List("viewA"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))),
          NodeToViewDefinition(List("viewB"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2), (B) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappings, List(
        NodeMappingDefinition(NodeTypeDefinition("A"), List(NodeToViewDefinition(List("viewA"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))),
        NodeMappingDefinition(NodeTypeDefinition("B"), List(NodeToViewDefinition(List("viewB"), Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses a relationship mapping definition") {
      val input =
        """|[a] FROM baz alias_baz
           |  START NODES (A, B) FROM foo alias_foo
           |      JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |          AND alias_foo.COLUMN_C = edge.COLUMN_D
           |  END NODES (C) FROM bar alias_bar
           |      JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipMappingDefinition, input, RelationshipMappingDefinition(
        relType = RelationshipTypeDefinition("a"),
        relTypeToView = List(RelationshipTypeToViewDefinition(
          viewDef = ViewDefinition(List("baz"), "alias_baz"),
          startNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("A", "B"),
            ViewDefinition(List("foo"), "alias_foo"),
            JoinOnDefinition(List(
              (List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A")),
              (List("alias_foo", "COLUMN_C"), List("edge", "COLUMN_D"))))),
          endNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("C"),
            ViewDefinition(List("bar"), "alias_bar"),
            JoinOnDefinition(List(
              (List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      ))))
    }

    it("parses a relationship mapping definition with custom property to column mapping") {
      val input =
        """|[a] FROM baz alias_baz ( colA AS foo, colB AS bar )
           |  START NODES (A, B) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |  END NODES   (C)    FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipMappingDefinition, input, RelationshipMappingDefinition(
        relType = RelationshipTypeDefinition("a"),
        relTypeToView = List(RelationshipTypeToViewDefinition(
          viewDef = ViewDefinition(List("baz"), "alias_baz"),
          maybePropertyMapping = Some(Map("foo" -> "colA", "bar" -> "colB")),
          startNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("A", "B"),
            ViewDefinition(List("foo"), "alias_foo"),
            JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
          endNodeTypeToView = NodeTypeToViewDefinition(
            NodeTypeDefinition("C"),
            ViewDefinition(List("bar"), "alias_bar"),
            JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
        ))))
    }

    it("parses a relationship label set definition") {
      val input =
        """|[TYPE_1]
           |  FROM baz edge
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz edge
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      val relMappingDef = RelationshipTypeToViewDefinition(
        viewDef = ViewDefinition(List("baz"), "edge"),
        startNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("A"),
          ViewDefinition(List("foo"), "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("B"),
          ViewDefinition(List("bar"), "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )

      success(
        relationshipMappingDefinition,
        input,
        RelationshipMappingDefinition(RelationshipTypeDefinition("TYPE_1"), List(relMappingDef, relMappingDef)))
    }

    it("parses relationship label sets") {
      val input =
        """|[TYPE_1]
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A,
           |
           |[TYPE_2]
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz alias_baz
           |    START NODES (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES   (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      val relMappingDef = RelationshipTypeToViewDefinition(
        viewDef = ViewDefinition(List("baz"), "alias_baz"),
        startNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("A"),
          ViewDefinition(List("foo"), "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeTypeToView = NodeTypeToViewDefinition(
          NodeTypeDefinition("B"),
          ViewDefinition(List("bar"), "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )

      success(relationshipMappings, input,
        List(
          RelationshipMappingDefinition(RelationshipTypeDefinition("TYPE_1"), List(relMappingDef, relMappingDef)),
          RelationshipMappingDefinition(RelationshipTypeDefinition("TYPE_2"), List(relMappingDef, relMappingDef))
        ))
    }
  }

  describe("parser error handling") {

    it("does not accept unknown types") {
      val ddlString =
        """|
           |CREATE ELEMENT TYPE (A {prop: char, prop2: int})
           |
           |CREATE GRAPH TYPE mySchema
           |
           |  (A);
           |
           |CREATE GRAPH myGraph WITH SCHEMA mySchema""".stripMargin

      an[DdlParsingException] shouldBe thrownBy {
        parse(ddlString)
      }
    }
  }

}
