package org.opencypher.spark.impl.spark

import org.apache.spark.sql.Row
import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}

class SparkCypherEngineTest extends SparkCypherTestSuite {

  import operations._

  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("filter operation on records") {

    val given = SparkCypherRecords.create(session.createDataFrame(Seq(
      (1, true, "Mats"),
      (2, false, "Martin"),
      (3, false, "Max"),
      (4, false, "Stefan")
    )).toDF("ID", "IS_SWEDE", "NAME"))

    val result = space.base.filter(given, Var("IS_SWEDE")(CTBoolean))

    result.toDF().count() should equal(1L)
  }

  test("select operation on records") {
    val given = SparkCypherRecords.create(session.createDataFrame(Seq(
      (1, true, "Mats"),
      (2, false, "Martin"),
      (3, false, "Max"),
      (4, false, "Stefan")
    )).toDF("ID", "IS_SWEDE", "NAME"))

    val result = space.base.select(given, IndexedSeq(Var("ID")(CTInteger), Var("NAME")(CTString)))

    result.toDF().columns should equal(Array("ID", "NAME"))
    result.toDF().orderBy("ID").collect() should equal(Array(
      Row(1, "Mats"),
      Row(2, "Martin"),
      Row(3, "Max"),
      Row(4, "Stefan")
    ))

    // TODO: test using Records equality
//    result.toDF() should equal(SparkCypherRecords.create(session.createDataFrame(Seq(
//      (1, "Mats"),
//      (2, "Martin"),
//      (3, "Max"),
//      (4, "Stefan")
//    )).toDF("ID", "NAME")))
  }
}
