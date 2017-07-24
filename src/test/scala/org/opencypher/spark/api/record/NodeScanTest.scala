package org.opencypher.spark.api.record

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTInteger, CTString}

class NodeScanTest extends SparkCypherTestSuite {
  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("test schema creation") {
    val nodeScan = NodeScan.on("p" -> "ID") {
      _.build
      .withImpliedLabel("A")
      .withImpliedLabel("B")
      .withOptionalLabel("C" -> "IS_C")
      .withPropertyKey("foo" -> "FOO")
      .withPropertyKey("bar" -> "BAR")
    }.from(SparkCypherRecords.create(
      Seq("ID", "IS_C", "FOO", "BAR"),
      Seq(
        (1, true, "Mats", 23)
      )
    ))

    nodeScan.schema should equal (Schema.empty
      .withImpliedLabel("A","B")
      .withImpliedLabel("B","A")
      .withImpliedLabel("C","A")
      .withImpliedLabel("C","B")
      .withLabelCombination("A","C")
      .withLabelCombination("B","C")
      .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTInteger)
      .withNodePropertyKeys("B")("foo" -> CTString.nullable, "bar" -> CTInteger)
    )
  }
}
