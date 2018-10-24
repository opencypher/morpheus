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

import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.okapi.testing.{BaseTestSuite, TestNameFixture}
import org.opencypher.sql.ddl.DdlParser._
import org.scalatest.mockito.MockitoSugar

class DdlParserTest extends BaseTestSuite with MockitoSugar with TestNameFixture {

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
      case Success(v, _) => v
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
  val emptySchemaDef: SchemaDefinition = SchemaDefinition()

  describe("property types") {
    it("parses STRING") {
      success(propertyType, CTString)
    }

    it("parses StRiNg") {
      success(propertyType, "StRiNg", CTString)
    }

    it("parses STRING?") {
      success(propertyType, "STRING?", CTString.nullable)
    }

    it("parses FOOBAR") {
      failure(propertyType)
    }
  }

  describe("properties") {
    it("parses key : FLOAT") {
      success(property, "key : FLOAT", "key" -> CTFloat)
    }

    it("parses key : FLOAT?") {
      success(property, "key : FLOAT?", "key" -> CTFloat.nullable)
    }

    it("parses key _ STRING") {
      failure(property)
    }

    it("parses { key : FLOAT }") {
      success(properties, "{ key : FLOAT }", Map("key" -> CTFloat))
    }

    it("parses { key1 : FLOAT, key2 : STRING }") {
      success(properties, "{ key1 : FLOAT, key2 : STRING }", Map("key1" -> CTFloat, "key2" -> CTString))
    }

    it("parses { }") {
      failure(properties)
    }
  }

  describe("label definitions") {
    it("parses LABEL (A)") {
      success(labelDefinition, LabelDefinition("A"))
    }

    it("parses LABEL  (A { foo : string? } )") {
      success(labelDefinition, LabelDefinition("A", Map("foo" -> CTString.nullable)))
    }
  }

  describe("key definitions") {
    it("parses KEY A (foo, bar)") {
      success(keyDefinition, "KEY A (foo, bar)", "A" -> Set("foo", "bar"))
    }

    it("parses KEY A ()") {
      failure(keyDefinition)
    }
  }

  describe("catalog label definition") {
    it("parses CATALOG CREATE LABEL (A)") {
      success(catalogLabelDefinition, LabelDefinition("A"))
    }

    it("parses CATALOG CREATE LABEL (A { foo : STRING })") {
      success(catalogLabelDefinition, LabelDefinition("A", Map("foo" -> CTString)))
    }

    it("parses CREATE LABEL (A  KEY  A_NK   (foo,   bar))") {
      success(catalogLabelDefinition, LabelDefinition("A", Map.empty, Some("A_NK" -> Set("foo", "bar"))))
    }

    it("parses CREATE LABEL (A { foo : STRING } KEY A_NK (foo,   bar))") {
      success(catalogLabelDefinition, LabelDefinition("A", Map("foo" -> CTString), Some("A_NK" -> Set("foo", "bar"))))
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
      success(schemaPatternDefinition, SchemaPatternDefinition(sourceLabelCombinations = Set(Set("A")), relTypes = Set("TYPE"), targetLabelCombinations = Set(Set("B"))))
    }

    it("parses (L1 | L2) <0 .. *> - [R1 | R2] -> <1>(L3)") {
      success(schemaPatternDefinition, SchemaPatternDefinition(
        Set(Set("L1"), Set("L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L1 | L2) - [R1 | R2] -> <1>(L3)") {
      success(schemaPatternDefinition, SchemaPatternDefinition(
        Set(Set("L1"), Set("L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L1, L2) - [R1 | R2] -> <1>(L3)") {
      success(schemaPatternDefinition, SchemaPatternDefinition(
        Set(Set("L1", "L2")),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set(Set("L3")))
      )
    }

    it("parses (L4 | L1, L2 | L3 & L5) - [R1 | R2] -> <1>(L3)") {
      success(schemaPatternDefinition, SchemaPatternDefinition(
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
           |CATALOG CREATE LABEL (A {name: STRING})
           |
           |CATALOG CREATE LABEL (B {sequence: INTEGER, nationality: STRING?, age: INTEGER?})
           |
           |CATALOG CREATE LABEL [TYPE_1]
           |
           |CATALOG CREATE LABEL [TYPE_2 {prop: BOOLEAN?}]""".stripMargin) shouldEqual
        DdlDefinitions(
          Some(SetSchemaDefinition("foo", Some("bar"))),
          List(
            LabelDefinition("A", Map("name" -> CTString)),
            LabelDefinition("B", Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
            LabelDefinition("TYPE_1"),
            LabelDefinition("TYPE_2", Map("prop" -> CTBoolean.nullable))
          ))
    }

    it("parses a schema with node, rel, and schema pattern definitions") {

      val expectedLocalLabelDefinitions = Set.empty[LabelDefinition]
      val expectedNodeDefs = Set(Set("A"), Set("B"), Set("A", "B"))
      val expectedRelDefs = Set("TYPE_1", "TYPE_2")
      val expectedPatternDefinitions = Set(
        SchemaPatternDefinition(
          Set(Set("A"), Set("B")),
          CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)),
          Set(Set("B"))),
        SchemaPatternDefinition(
          Set(Set("A")),
          CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(0, None),
          Set(Set("A")))
      )

      globalSchemaDefinition.parse(
        """|CREATE GRAPH SCHEMA mySchema
           |
           |  --NODES
           |  (A),
           |  (B),
           |  (A, B)
           |
           |  --EDGES
           |  [TYPE_1],
           |  [TYPE_2]
           |
           |  (A | B) <0 .. *> - [TYPE_1] -> <1> (B),
           |  (A) <*> - [TYPE_1] -> (A);
        """.stripMargin) should matchPattern {
        case Success(("mySchema", SchemaDefinition(`expectedLocalLabelDefinitions`, `expectedNodeDefs`, `expectedRelDefs`, `expectedPatternDefinitions`)), _) =>
      }
    }

    it("parses CREATE GRAPH SCHEMA mySchema (A)-[TYPE]->(B)") {
      success(globalSchemaDefinition, ("mySchema",
        SchemaDefinition(schemaPatternDefinitions = Set(
          SchemaPatternDefinition(sourceLabelCombinations = Set(Set("A")), relTypes = Set("TYPE"), targetLabelCombinations = Set(Set("B")))
        ))))
    }
  }

  describe("graph definitions") {
    it("parses CREATE GRAPH myGraph WITH GRAPH SCHEMA foo") {
      success(graphDefinition, GraphDefinition("myGraph", Some("foo")))
    }

    it("parses CREATE GRAPH myGraph WITH GRAPH SCHEMA mySchema") {
      success(graphDefinition, GraphDefinition("myGraph", Some("mySchema")))
    }

    it("parses a graph definition with inlined schema") {
      val expectedSchemaDefinition = SchemaDefinition(
        localLabelDefinitions = Set(LabelDefinition("A"), LabelDefinition("B")),
        nodeDefinitions = Set(Set("A", "B")),
        relDefinitions = Set("B")
      )
      graphDefinition.parse(
        """|CREATE GRAPH myGraph WITH GRAPH SCHEMA (
           | LABEL (A),
           | LABEL (B)
           |
           | (A,B)
           | [B]
           |)
        """.stripMargin) should matchPattern {
        case Success(GraphDefinition("myGraph", None, `expectedSchemaDefinition`, `emptyList`, `emptyList`), _) =>
      }
    }
  }

  describe("NODE LABEL SETS | RELATIONSHIP LABEL SETS") {

    it("parses (A) FROM view") {
      success(nodeMappingDefinition, NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("view"))))
    }

    it("parses (A) FROM view (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappingDefinition, NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("view", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))))
    }
    it("parses (A) FROM viewA FROM viewB") {
      success(nodeMappingDefinition, NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("viewA"), NodeToViewDefinition("viewB"))))
    }

    it("parses NODE LABEL SETS ( (A) FROM viewA (B) FROM viewB )") {
      success(nodeMappings, List(NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("viewA"))), NodeMappingDefinition(Set("B"), List(NodeToViewDefinition("viewB")))))
    }

    it("parses NODE LABEL SETS ( (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2) )") {
      success(nodeMappings, List(
        NodeMappingDefinition(Set("A"), List(
          NodeToViewDefinition("viewA", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))),
          NodeToViewDefinition("viewB", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses NODE LABEL SETS ( (A) FROM viewA (column1 AS propertyKey1, column2 AS propertyKey2) (B) FROM viewB (column1 AS propertyKey1, column2 AS propertyKey2) )") {
      success(nodeMappings, List(
        NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("viewA", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))),
        NodeMappingDefinition(Set("B"), List(NodeToViewDefinition("viewB", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2")))))
      ))
    }

    it("parses JOIN ON view_a.COLUMN_A = view_b.COLUMN_B") {
      success(joinOnDefinition, JoinOnDefinition(List((List("view_a", "COLUMN_A"), List("view_b", "COLUMN_B")))))
    }

    it("parses JOIN ON view_a.COLUMN_A = view_b.COLUMN_B AND view_a.COLUMN_C = view_b.COLUMN_D") {
      success(joinOnDefinition, JoinOnDefinition(List(
        (List("view_a", "COLUMN_A"), List("view_b", "COLUMN_B")),
        (List("view_a", "COLUMN_C"), List("view_b", "COLUMN_D")))))
    }

    it("parses LABEL SET (A, B) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A") {
      success(labelToViewDefinition, LabelToViewDefinition(
        Set("A", "B"),
        ViewDefinition("foo", "alias_foo"),
        JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )
    }

    it("parses a relationship mapping definition") {
      val input =
        """|FROM baz alias_baz
           |  START NODES
           |    LABEL SET (A, B) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |  END NODES
           |    LABEL SET (C) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipToViewDefinition, input, RelationshipToViewDefinition(
        viewDefinition = ViewDefinition("baz", "alias_baz"),
        startNodeToViewDefinition = LabelToViewDefinition(
          Set("A", "B"),
          ViewDefinition("foo", "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeToViewDefinition = LabelToViewDefinition(
          Set("C"),
          ViewDefinition("bar", "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      ))
    }

    it("parses a relationship mapping definition with custom property to column mapping") {
      val input =
        """|FROM baz alias_baz ( colA AS foo, colB AS bar )
           |  START NODES
           |    LABEL SET (A, B) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |  END NODES
           |    LABEL SET (C) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      success(relationshipToViewDefinition, input, RelationshipToViewDefinition(
        viewDefinition = ViewDefinition("baz", "alias_baz"),
        maybePropertyMapping = Some(Map("foo" -> "colA", "bar" -> "colB")),
        startNodeToViewDefinition = LabelToViewDefinition(
          Set("A", "B"),
          ViewDefinition("foo", "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeToViewDefinition = LabelToViewDefinition(
          Set("C"),
          ViewDefinition("bar", "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      ))
    }

    it("parses a relationship label set definition") {
      val input =
        """|(TYPE_1)
           |  FROM baz edge
           |    START NODES
           |      LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES
           |      LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |  FROM baz alias_baz
           |    START NODES
           |      LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |    END NODES
           |      LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
        """.stripMargin

      val relMappingDef = RelationshipToViewDefinition(
        viewDefinition = ViewDefinition("baz", "edge"),
        startNodeToViewDefinition = LabelToViewDefinition(
          Set("A"),
          ViewDefinition("foo", "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeToViewDefinition = LabelToViewDefinition(
          Set("B"),
          ViewDefinition("bar", "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )
      success(relationshipMappingDefinition, input, RelationshipMappingDefinition("TYPE_1", List(relMappingDef, relMappingDef)))
    }

    it("parses relationship label sets") {
      val input =
        """|RELATIONSHIP LABEL SETS (
           |
           |        (TYPE_1)
           |          FROM baz alias_baz
           |            START NODES
           |              LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |            END NODES
           |              LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |          FROM baz alias_baz
           |            START NODES
           |              LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |            END NODES
           |              LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |
           |        (TYPE_2)
           |          FROM baz alias_baz
           |            START NODES
           |              LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |            END NODES
           |              LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |          FROM baz alias_baz
           |            START NODES
           |              LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
           |            END NODES
           |              LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
           |
           |
           |    )
        """.stripMargin

      val relMappingDef = RelationshipToViewDefinition(
        viewDefinition = ViewDefinition("baz", "alias_baz"),
        startNodeToViewDefinition = LabelToViewDefinition(
          Set("A"),
          ViewDefinition("foo", "alias_foo"),
          JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
        endNodeToViewDefinition = LabelToViewDefinition(
          Set("B"),
          ViewDefinition("bar", "alias_bar"),
          JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
      )

      success(relationshipMappings, input,
        List(
          RelationshipMappingDefinition("TYPE_1", List(relMappingDef, relMappingDef)),
          RelationshipMappingDefinition("TYPE_2", List(relMappingDef, relMappingDef))
        ))
    }
  }

  it("parses correct schema") {
    val ddlDefinition = parse(
      """|SET SCHEMA foo.bar;
         |
         |CATALOG CREATE LABEL (A {name: STRING})
         |
         |CATALOG CREATE LABEL (B {sequence: INTEGER, nationality: STRING?, age: INTEGER?})
         |
         |CATALOG CREATE LABEL [TYPE_1]
         |
         |CATALOG CREATE LABEL [TYPE_2 {prop: BOOLEAN?}]
         |
         |CREATE GRAPH SCHEMA mySchema
         |
         |  LABEL (A { foo : INTEGER } ),
         |  LABEL (C)
         |
         |
         |  -- nodes
         |  (A),
         |  (B),
         |  (A, B),
         |  (C)
         |
         |
         |  -- edges
         |  [TYPE_1],
         |  [TYPE_2]
         |
         |  -- schema patterns
         |  (A) <0 .. *> - [TYPE_1] -> <1> (B);
         |
         |CREATE GRAPH myGraph WITH GRAPH SCHEMA mySchema
         |  NODE LABEL SETS (
         |    (A) FROM foo
         |  )
         |
         |  RELATIONSHIP LABEL SETS (
         |
         |        (TYPE_1)
         |          FROM baz alias_baz
         |            START NODES
         |              LABEL SET (A) FROM foo alias_foo JOIN ON alias_foo.COLUMN_A = edge.COLUMN_A
         |            END NODES
         |              LABEL SET (B) FROM bar alias_bar JOIN ON alias_bar.COLUMN_A = edge.COLUMN_A
         |    )
      """.stripMargin)

    ddlDefinition should equalWithTracing(
      DdlDefinitions(
        setSchema = Some(SetSchemaDefinition("foo", Some("bar"))),
        labelDefinitions = List(
          LabelDefinition("A", Map("name" -> CTString)),
          LabelDefinition("B", Map("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)),
          LabelDefinition("TYPE_1"),
          LabelDefinition("TYPE_2", Map("prop" -> CTBoolean.nullable))
        ),
        schemaDefinitions = Map("mySchema" -> SchemaDefinition(
          localLabelDefinitions = Set(
            LabelDefinition("A", properties = Map("foo" -> CTInteger)),
            LabelDefinition("C")),
          nodeDefinitions = Set(Set("A"), Set("B"), Set("A", "B"), Set("C")),
          relDefinitions = Set("TYPE_1", "TYPE_2"),
          schemaPatternDefinitions = Set(SchemaPatternDefinition(Set(Set("A")), CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)), Set(Set("B")))))),
        graphDefinitions = List(GraphDefinition(
          name = "myGraph",
          maybeSchemaName = Some("mySchema"),
          localSchemaDefinition = emptySchemaDef,
          nodeMappings = List(NodeMappingDefinition(Set("A"), List(NodeToViewDefinition("foo")))),
          relationshipMappings = List(RelationshipMappingDefinition("TYPE_1", List(RelationshipToViewDefinition(
            viewDefinition = ViewDefinition("baz", "alias_baz"),
            startNodeToViewDefinition = LabelToViewDefinition(
              Set("A"),
              ViewDefinition("foo", "alias_foo"),
              JoinOnDefinition(List((List("alias_foo", "COLUMN_A"), List("edge", "COLUMN_A"))))),
            endNodeToViewDefinition = LabelToViewDefinition(
              Set("B"),
              ViewDefinition("bar", "alias_bar"),
              JoinOnDefinition(List((List("alias_bar", "COLUMN_A"), List("edge", "COLUMN_A")))))
          ))))))
      )
    )

    ddlDefinition.graphSchemas shouldEqual Map(
      "myGraph" -> Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys(Set("C"))
        .withRelationshipType("TYPE_1")
        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTBoolean.nullable)
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    )
  }

  describe("OKAPI schema conversion") {

    it("throws if a label is not defined") {
      val ddlDefinition = parse(
        """|CATALOG CREATE LABEL (A)
           |
           |CREATE GRAPH SCHEMA mySchema
           |
           |  LABEL (B)
           |
           |  -- (illegal) node definition
           |  (C)
           |
           |CREATE GRAPH myGraph WITH GRAPH SCHEMA mySchema
        """.stripMargin)

      an[IllegalArgumentException] shouldBe thrownBy {
        ddlDefinition.graphSchemas
      }
    }

    it("throws if a relationship type is not defined") {
      val ddlDefinition = parse(
        """|CATALOG CREATE LABEL (A)
           |
           |CREATE GRAPH SCHEMA mySchema
           |
           |  LABEL (B)
           |
           |  -- (illegal) relationship type definition
           |  [C]
           |
           |CREATE GRAPH myGraph WITH GRAPH SCHEMA mySchema
        """.stripMargin)

      an[IllegalArgumentException] shouldBe thrownBy {
        ddlDefinition.graphSchemas
      }
    }

    it("throws if a undefined label is used") {
      val ddlString =
        """|CREATE GRAPH SCHEMA mySchema
           |  (A)-[T]->(A);
           |
           |CREATE GRAPH myGraph WITH GRAPH SCHEMA mySchema""".stripMargin

      an[IllegalArgumentException] shouldBe thrownBy {
        parse(ddlString).graphSchemas
      }
    }
  }

  describe("parser error handling") {

    it("does not accept unknown types") {
      val ddlString =
        """|
           |CATALOG CREATE LABEL (A {prop: char, prop2: int})
           |
           |CREATE GRAPH SCHEMA mySchema
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
