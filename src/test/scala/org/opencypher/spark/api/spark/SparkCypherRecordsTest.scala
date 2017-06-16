package org.opencypher.spark.api.spark

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{Label, PropertyKey, TokenRegistry}
import org.opencypher.spark.api.record.{EmbeddedNode, EmbeddedRelationship, OpaqueField, ProjectedExpr}
import org.opencypher.spark.api.types.{CTBoolean, CTNode, CTRelationship, CTString}
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

    // TODO: Do we really need to track negative label information?
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
      EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relTypes("COLOR", "red", "blue", "green", "yellow").build
    )

    val entity = Var("r")(CTRelationship("red", "blue", "green", "yellow"))

    // TODO: Use schema for determining more precise node types
    result.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entity),
      ProjectedExpr(StartNode(entity)(CTNode)),
      ProjectedExpr(EndNode(entity)(CTNode)),
      ProjectedExpr(TypeId(entity)(CTRelationship("red", "blue", "green", "yellow")))
    ))
  }
}
