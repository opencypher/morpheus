package org.opencypher.sql.ddl

import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.sql.ddl.DdlParser._
import org.scalatest.mockito.MockitoSugar

class DdlSchemaTest extends BaseTestSuite with MockitoSugar {

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
        "CREATE GRAPH myGraph WITH SCHEMA foo NODE LABEL SETS ()") should matchPattern {
        case Success(GraphDefinition("myGraph", Some("foo"), `emptySchemaDef`, `emptyList`), _) =>
      }
    }

    it("parses a graph definition with a schema reference") {
      graphDefinition.parse(
        "CREATE GRAPH myGraph WITH SCHEMA mySchema NODE LABEL SETS ()") should matchPattern {
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
           |NODE LABEL SETS ()
        """.stripMargin) should matchPattern {
        case Success(GraphDefinition("myGraph", None, `expectedSchemaDefinition`, `emptyList`), _) =>
      }
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

    ddlDefinition.globalSchemas shouldEqual Map(
      "mySchema" -> Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTInteger)
        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
        .withNodePropertyKeys(Set("C"))
        .withRelationshipType("TYPE_1")
        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTBoolean.nullable)
        .withSchemaPatterns(SchemaPattern("A", "TYPE_1", "B"))
    )
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
