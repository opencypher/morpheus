package org.opencypher.sql.ddl

import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.types.{CTFloat, CTString, CypherType}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.sql.ddl.DdlParser._
import org.scalatest.mockito.MockitoSugar

import scala.collection.Map

class DdlSchemaTest extends BaseTestSuite with MockitoSugar {

  val emptyMap = Map.empty[String, CypherType]

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
      labelDefinition.parse("LABEL (A)") should matchPattern {
        case Success(`expected`, _) =>
      }
    }

    it("parses node labels with properties") {
      val expected = "A" -> Map("foo" -> CTString.nullable)
      labelDefinition.parse("LABEL  (A { foo : string? } )") should matchPattern {
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
      createLabelStmt.parse("CATALOG CREATE LABEL (A)") should matchPattern {
        case Success(LabelDeclaration("A", `emptyMap`, None), _) =>
      }
    }

    it("parses CREATE LABEL <labelDefinition>") {
      createLabelStmt.parse("CREATE LABEL (A)") should matchPattern {
        case Success(LabelDeclaration("A", `emptyMap`, None), _) =>
      }
    }

    it("parses CREATE LABEL <labelDefinition> KEY <keyDefinition>") {
      val expectedKeyDefinition = "A_NK" -> Set("foo", "bar")
      createLabelStmt.parse("CREATE LABEL (A) KEY A_NK (foo, bar)") should matchPattern {
        case Success(LabelDeclaration("A", `emptyMap`, Some(`expectedKeyDefinition`)), _) =>
      }
    }
  }


  it("parses correct schema") {
    val sqlDdl =
      """
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
        |  --NODES
        |  (A),
        |  (B),
        |  (A, B)
        |
        |  --EDGES
        |  [TYPE_1],
        |  [TYPE_2];
        |
        |CREATE GRAPH myGraph WITH SCHEMA mySchema
      """.stripMargin

    //    val ddl = DdlParser.parse(sqlDdl)
    //
    //    ddl.show()


    //    sqlGraphSource(sqlDdl).schema(GraphName("myGraph")).get should equal(
    //      Schema.empty.withNodePropertyKeys("A")("name" -> CTString)
    //        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
    //        .withNodePropertyKeys("A", "B")("name" -> CTString, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
    //        .withRelationshipPropertyKeys("TYPE_1")()
    //        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTBoolean.nullable)
    //    )
  }

  // TODO: Enable and fix once NEN patterns are supported in Schema trait
  ignore("parses correct schema with NEN patterns") {
    val sqlDdl =
      """
        |CATALOG CREATE LABEL A
        |  PROPERTIES (
        |    name CHAR(10) NOT NULL
        |  );
        |
        |CATALOG CREATE LABEL B
        |  PROPERTIES (
        |    sequence INTEGER NOT NULL,
        |    nationality CHAR(3),
        |    age INTEGER
        |  );
        |
        |CATALOG CREATE LABEL TYPE_1;
        |
        |CATALOG CREATE LABEL TYPE_2
        |  PROPERTIES (
        |    prop INTEGER
        |  );
        |
        |CREATE GRAPH SCHEMA mySchema
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
        |  (A) <0 .. *> - [TYPE_1] -> <1> (B);
        |
        |CREATE GRAPH myGraph WITH SCHEMA mySchema
      """.stripMargin

    //    sqlGraphSource(sqlDdl).schema(GraphName("myGraph")).get should equal(
    //      Schema.empty.withNodePropertyKeys("A")("name" -> CTString)
    //        .withNodePropertyKeys("B")("sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
    //        .withNodePropertyKeys("A", "B")("name" -> CTString, "sequence" -> CTInteger, "nationality" -> CTString.nullable, "age" -> CTInteger.nullable)
    //        .withRelationshipPropertyKeys("TYPE_1")()
    //        .withRelationshipPropertyKeys("TYPE_2")("prop" -> CTInteger.nullable)
    //      // .withConstraint(A, TYPE_1, B) or whatever
    //    )
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
