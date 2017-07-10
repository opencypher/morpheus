package org.opencypher.spark.impl.spark

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}

class SparkCypherEngineOperationsTest extends SparkCypherTestSuite {

  import operations._

  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("filter operation on records") {

    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
    ))

    val result = space.base.filter(given, Var("IS_SWEDE")(CTBoolean))

    result.toDF().count() should equal(1L)
  }

  test("select operation on records") {
    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
    ))

    val result = space.base.select(given, IndexedSeq(Var("ID")(CTInteger), Var("NAME")(CTString)))

    result shouldMatch SparkCypherRecords.create(
      Seq("ID", "NAME"), Seq(
        (1, "Mats"),
        (2, "Martin"),
        (3, "Max"),
        (4, "Stefan")
    ))
  }
}
