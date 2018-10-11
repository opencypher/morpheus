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
import org.opencypher.okapi.testing.{BaseTestSuite, TestNameFixture}
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.sql.ddl.DdlParser._
import org.scalatest.mockito.MockitoSugar

class DdlSchemaTest extends BaseTestSuite with MockitoSugar with TestNameFixture {

  override val separator = "parses "

  private def success[T, Elem](parser: fastparse.core.Parser[T, Elem, String], expectation: T): Unit = {
    parser.parse(testName) should matchPattern {
      case Success(`expectation`, _) =>
    }
  }

  private def failure[T, Elem](parser: fastparse.core.Parser[T, Elem, String]): Unit = {
    parser.parse(testName) should matchPattern {
      case Failure(_, _, _) =>
    }
  }

  val emptyMap = Map.empty[String, CypherType]
  val emptyList: List[Nothing] = List.empty[Nothing]
  val emptySchemaDef: SchemaDefinition = SchemaDefinition()

  describe("property types") {
    it("parses valid property types") {
      propertyType.parse("STRING") should matchPattern { case Success(CTString, _) => }
    }

    it("parses valid property types ignoring the case") {
      propertyType.parse("StRiNg") should matchPattern { case Success(CTString, _) => }
    }

    it("parses valid nullable property types") {
      propertyType.parse("STRING?") should matchPattern { case Success(CTString.nullable, _) => }
    }

    it("fails parsing invalid property types") {
      propertyType.parse("FOOBAR") should matchPattern { case Failure(_, _, _) => }
    }
  }

  describe("properties") {
    it("parses valid properties") {
      val expectedProperty = "key" -> CTFloat
      property.parse("key : FLOAT") should matchPattern { case Success(`expectedProperty`, _) => }
    }

    it("parses valid nullable properties") {
      val expectedProperty = "key" -> CTFloat.nullable
      property.parse("key : FLOAT?") should matchPattern { case Success(`expectedProperty`, _) => }
    }

    it("fails parsing invalid properties") {
      property.parse("key _ STRING") should matchPattern { case Failure(_, _, _) => }
    }

    it("parses single property in curlies") {
      val expectedProperties = Map("key" -> CTFloat)
      properties.parse("{ key : FLOAT }") should matchPattern { case Success(`expectedProperties`, _) => }
    }

    it("parses multiple properties in curlies") {
      val expectedProperties = Map("key1" -> CTFloat, "key2" -> CTString)
      properties.parse("{ key1 : FLOAT, key2 : STRING }") should matchPattern {
        case Success(`expectedProperties`, _) =>
      }
    }

    it("fails parsing empty properties") {
      properties.parse("{ }") should matchPattern { case Failure(_, _, _) => }
    }
  }

  describe("label definitions") {
    it("parses node labels without properties") {
      val expected = "A" -> emptyMap
      labelWithoutKeys.parse("LABEL (A)") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses node labels with properties") {
      val expected = "A" -> Map("foo" -> CTString.nullable)
      labelWithoutKeys.parse("LABEL  (A { foo : string? } )") should matchPattern {
        case Success(`expected`, _) =>
      }
    }
  }

  describe("key definitions") {
    it("parses a key definition") {
      val expectedDefinition = "A" -> Set("foo", "bar")
      keyDefinition.parse("KEY A (foo, bar)") should matchPattern {
        case Success(`expectedDefinition`, _) =>
      }
    }

    it("fails parsing empty property lists") {
      keyDefinition.parse("KEY A ()") should matchPattern { case Failure(_, _, _) => }
    }
  }

  describe("[CATALOG] CREATE LABEL <labelDefinition|relTypeDefinition> [KEY keyDefinition]") {
    it("parses CATALOG CREATE LABEL <labelDefinition>") {
      catalogLabelDefinition.parse("CATALOG CREATE LABEL (A)") should matchPattern {
        case Success(LabelDefinition("A", `emptyMap`, None), _) =>
      }
    }

    it("parses CREATE LABEL <labelDefinition>") {
      catalogLabelDefinition.parse("CREATE LABEL (A)") should matchPattern {
        case Success(LabelDefinition("A", `emptyMap`, None), _) =>
      }
    }

    it("parses CREATE LABEL <labelDefinition> KEY <keyDefinition>") {
      val expectedKeyDefinition = "A_NK" -> Set("foo", "bar")
      catalogLabelDefinition.parse("CREATE LABEL (A) KEY A_NK (foo, bar)") should matchPattern {
        case Success(LabelDefinition("A", `emptyMap`, Some(`expectedKeyDefinition`)), _) =>
      }
    }

  }

  describe("schema pattern definitions") {

    it("parses <1>") {
      val expected = CardinalityConstraint(1, Some(1))
      cardinalityConstraint.parse("<1>") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses <1, *>") {
      val expected = CardinalityConstraint(1, None)
      cardinalityConstraint.parse("<1, *>") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses <1 .. *>") {
      val expected = CardinalityConstraint(1, None)
      cardinalityConstraint.parse("<1 .. *>") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses <*>") {
      val expected = CardinalityConstraint(0, None)
      cardinalityConstraint.parse("<*>") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses <1, 3>") {
      val expected = CardinalityConstraint(1, Some(3))
      cardinalityConstraint.parse("<1, 3>") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses schema pattern definitions") {
      val expected = SchemaPatternDefinition(
        Set("L1", "L2"),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set("L3"))
      schemaPatternDefinition.parse("(L1 | L2) <0 .. *> - [R1 | R2] -> <1>(L3)") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses schema pattern definitions with implicit cardinality constraint") {
      val expected = SchemaPatternDefinition(
        Set("L1", "L2"),
        CardinalityConstraint(0, None), Set("R1", "R2"), CardinalityConstraint(1, Some(1)),
        Set("L3"))
      schemaPatternDefinition.parse("(L1 | L2) - [R1 | R2] -> <1>(L3)") should matchPattern {
        case Success(`expected`, _) =>
      }
    }
  }

  describe("schema definitions") {

    it("parses multiple label definitions") {
      parse(
        """|CATALOG CREATE LABEL (A {name: STRING})
           |
           |CATALOG CREATE LABEL (B {sequence: INTEGER, nationality: STRING?, age: INTEGER?})
           |
           |CATALOG CREATE LABEL [TYPE_1]
           |
           |CATALOG CREATE LABEL [TYPE_2 {prop: BOOLEAN?}]""".stripMargin) shouldEqual
        DdlDefinitions(List(
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
          Set("A", "B"),
          CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)),
          Set("B")),
        SchemaPatternDefinition(
          Set("A"),
          CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(0, None),
          Set("A"))
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
  }

  describe("graph definitions") {
    it("parses a graph definition") {
      graphDefinition.parse(
        "CREATE GRAPH myGraph WITH SCHEMA foo") should matchPattern {
        case Success(GraphDefinition("myGraph", Some("foo"), `emptySchemaDef`, `emptyList`), _) =>
      }
    }

    it("parses a graph definition with a schema reference") {
      graphDefinition.parse(
        "CREATE GRAPH myGraph WITH SCHEMA mySchema") should matchPattern {
        case Success(GraphDefinition("myGraph", Some("mySchema"), `emptySchemaDef`, `emptyList`), _) =>
      }
    }

    it("parses a graph definition with inlined schema") {
      val expectedSchemaDefinition = SchemaDefinition(
        localLabelDefinitions = Set(LabelDefinition("A"), LabelDefinition("B")),
        nodeDefinitions = Set(Set("A", "B")),
        relDefinitions = Set("B")
      )
      graphDefinition.parse(
        """|CREATE GRAPH myGraph WITH SCHEMA (
           | LABEL (A),
           | LABEL (B)
           |
           | (A,B)
           | [B]
           |)
        """.stripMargin) should matchPattern {
        case Success(GraphDefinition("myGraph", None, `expectedSchemaDefinition`, `emptyList`), _) =>
      }
    }
  }

  describe("NODE LABEL SETS | RELATIONSHIP LABEL SETS") {

    it("parses (A) FROM view") {
      success(nodeMappingDefinition, NodeMappingDefinition(Set("A"), "view"))
    }

    it("parses (A) FROM view (column1 AS propertyKey1, column2 AS propertyKey2)") {
      success(nodeMappingDefinition, NodeMappingDefinition(Set("A"), "view", Some(Map("propertyKey1" -> "column1", "propertyKey2" -> "column2"))))
    }

    it("parses single JOIN ON") {
      joinTuples.parse("JOIN ON view_a.COLUMN_A = view_b.COLUMN_B") should matchPattern {
        case Success(JoinOnDefinition(List((List("view_a", "COLUMN_A"), List("view_b", "COLUMN_B")))), _) =>
      }
    }

    it("parses multiple JOIN ON") {
      joinTuples.parse(
        """|JOIN ON view_a.COLUMN_A = view_b.COLUMN_B
           |AND view_a.COLUMN_C = view_b.COLUMN_D
        """.stripMargin) should matchPattern {
        case Success(JoinOnDefinition(List(
        (List("view_a", "COLUMN_A"), List("view_b", "COLUMN_B")),
        (List("view_a", "COLUMN_C"), List("view_b", "COLUMN_D")))), _) =>
      }
    }

    it("parses mapping definition") {
      //      START NODES
      //        LABEL SET (Resident, Person)
      //      FROM VIEW_RESIDENT start_nodes
      //      JOIN ON start_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
    }

    ignore("parses") {
      val input =
        """|RELATIONSHIP LABEL SETS (
           |
           |        (PRESENT_IN)
           |            FROM VIEW_RESIDENT_ENUMERATED_IN_TOWN edge
           |                START NODES
           |                    LABEL SET (Resident, Person)
           |                    FROM VIEW_RESIDENT start_nodes
           |                        JOIN ON start_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
           |                END NODES
           |                    LABEL SET (Town)
           |                    FROM TOWN end_nodes
           |                        JOIN ON end_nodes.REGION = edge.REGION
           |                        AND end_nodes.CITY_NAME = edge.CITY_NAME,
           |            FROM VIEW_VISITOR_ENUMERATED_IN_TOWN edge
           |                START NODES
           |                    LABEL SET (Visitor, Person)
           |                    FROM VIEW_VISITOR start_nodes
           |                        JOIN ON start_nodes.NATIONALITY = edge.COUNTRYOFORIGIN
           |                        AND start_nodes.PASSPORT_NUMBER = edge.PASSPORT_NO
           |                END NODES
           |                    LABEL SET (Town)
           |                    FROM TOWN end_nodes
           |                        JOIN ON end_nodes.REGION = edge.REGION
           |                        AND end_nodes.CITY_NAME = edge.CITY_NAME,
           |
           |        (LICENSED_BY)
           |            FROM VIEW_LICENSED_DOG edge
           |                START NODES
           |                    LABEL SET (LicensedDog)
           |                    -- edge table is also start table, each row joining to itself
           |                    FROM VIEW_LICENSED_DOG start_nodes
           |                        JOIN ON start_nodes.LICENCE_NUMBER = edge.LICENCE_NUMBER
           |                END NODES
           |                    LABEL SET (Resident, Person)
           |                    FROM VIEW_RESIDENT end_nodes
           |                        JOIN ON end_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
           |    )
        """.stripMargin
    }
  }

  it("parses correct schema") {
    val ddlDefinition = parse(
      """|CATALOG CREATE LABEL (A {name: STRING})
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
         |CREATE GRAPH myGraph WITH SCHEMA mySchema
         |  NODE LABEL SETS (
         |    (A) FROM foo
         |  )
         |
      """.stripMargin)
    ddlDefinition should equalWithTracing(
      DdlDefinitions(
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
          schemaPatternDefinitions = Set(SchemaPatternDefinition(Set("A"), CardinalityConstraint(0, None), Set("TYPE_1"), CardinalityConstraint(1, Some(1)), Set("B"))))),
        graphDefinitions = List(GraphDefinition("myGraph", Some("mySchema"), emptySchemaDef, List(NodeMappingDefinition(Set("A"), "foo"))))
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

    it("merges global and inlined schema definitions") {
      val ddlDefinition = parse(
        """|CREATE GRAPH SCHEMA mySchema
           |  LABEL (A)
           |  (A)
           |CREATE GRAPH myGraph WITH SCHEMA mySchema (
           |  LABEL (B)
           |  (B)
           |)
        """.stripMargin)

      ddlDefinition.graphSchemas shouldEqual Map(
        "myGraph" -> Schema.empty
          .withNodePropertyKeys(Set("A"))
          .withNodePropertyKeys(Set("B"))
      )
    }

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
           |CREATE GRAPH myGraph WITH SCHEMA mySchema
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
           |CREATE GRAPH myGraph WITH SCHEMA mySchema
        """.stripMargin)

      an[IllegalArgumentException] shouldBe thrownBy {
        ddlDefinition.graphSchemas
      }
    }
  }


  it("does not accept unknown types") {
    val ddl =
      """
        |CATALOG CREATE LABEL (A {prop: char, prop2: int})
        |
        |CREATE GRAPH SCHEMA mySchema
        |
        |  (A);
        |
        |CREATE GRAPH myGraph WITH SCHEMA mySchema
      """.stripMargin

    //    an[IllegalArgumentException] shouldBe thrownBy {
    //      sqlGraphSource(ddl).schema(GraphName("myGraph")).get
    //    }
  }

  //  private def sqlGraphSource(ddl: String, metaDataFinder: Option[MetaDataFinder] = None): SqlGraphSource = {
  //    val sqlDataSources: Map[QualifiedSqlName, SQLDataSource] = Map(new QualifiedSqlNameImpl("datasource", "schema") -> SQLDataSource("hive", "myname", ""))
  //
  //    implicit val session = mock[CAPSSession]
  //    new SqlGraphSource(ddl, sqlDataSources, metaDataFinder.getOrElse(SourceMetaDataFinder(sqlDataSources)))
  //  }

}
