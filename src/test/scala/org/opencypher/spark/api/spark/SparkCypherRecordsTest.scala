package org.opencypher.spark.api.spark

import org.apache.spark.sql.Row
import org.opencypher.spark.api.exception.SparkCypherException
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{Label, PropertyKey, TokenRegistry}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types._
import org.opencypher.spark.{TestSession, TestSuiteImpl}

class SparkCypherRecordsTest extends TestSuiteImpl with TestSession.Fixture {

  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("contract nodes") {
    val given = SparkCypherRecords.create(session.createDataFrame(Seq(
      (1, true, "Mats"),
      (2, false, "Martin"),
      (3, false, "Max"),
      (4, false, "Stefan")
    )).toDF("ID", "IS_SWEDE", "NAME"))

    val result = given.contract(
      EmbeddedNode("n" -> "ID").build
        .withImpliedLabel("Person")
        .withOptionalLabel("Swedish" -> "IS_SWEDE")
        .withProperty("name" -> "NAME")
        .verify
    )

    val entity = Var("n")(CTNode(Map("Person" -> true)))

    result.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entity),
      ProjectedExpr(HasLabel(entity, Label("Swedish"))(CTBoolean)),
      ProjectedExpr(Property(entity, PropertyKey("name"))(CTString.nullable))
    ))
  }

  test("contract relationships with a fixed type") {

    val given = SparkCypherRecords.create(session.createDataFrame(Seq(
      (10, 1, 2, "red"),
      (11, 2, 3, "blue"),
      (12, 3, 4, "green"),
      (13, 4, 1, "yellow")
    )).toDF("ID", "FROM", "TO", "COLOR"))

    val result = given.contract(
      EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relType("NEXT").build
        .withProperty("color" -> "COLOR")
        .verify
    )

    val entity = Var("r")(CTRelationship("NEXT"))

    result.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entity),
      ProjectedExpr(StartNode(entity)(CTNode)),
      ProjectedExpr(EndNode(entity)(CTNode)),
      ProjectedExpr(Property(entity, PropertyKey("color"))(CTString.nullable))
    ))
  }

  test("contract relationships with a dynamic type") {
    // TODO: Reject records using unknown tokes
    val given = SparkCypherRecords.create(session.createDataFrame(Seq(
      (10, 1, 2, 50),
      (11, 2, 3, 51),
      (12, 3, 4, 52),
      (13, 4, 1, 53)
    )).toDF("ID", "FROM", "TO", "COLOR"))

    val result = given.contract(
      EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relTypes("COLOR", "RED", "BLUE", "GREEN", "YELLOW").build
    )

    val entity = Var("r")(CTRelationship("RED", "BLUE", "GREEN", "YELLOW"))

    // TODO: Use schema for determining more precise node types
    result.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entity),
      ProjectedExpr(StartNode(entity)(CTNode)),
      ProjectedExpr(EndNode(entity)(CTNode)),
      ProjectedExpr(TypeId(entity)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW")))
    ))
  }

  test("can not construct records with data/header column name conflict") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")()), OpaqueField(Var("notString")()))

    a [SparkCypherException] shouldBe thrownBy {
      SparkCypherRecords.create(header, data)
    }
  }

  test("can construct records with matching data/header") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")(CTInteger)), OpaqueField(Var("string")(CTString)))

    val records = SparkCypherRecords.create(header, data) // no exception is thrown
    records.data.select("int").collect() should equal(Array(Row(1), Row(2)))
  }
}
