package org.opencypher.spark.api.io.sql

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.HiveFormat
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.CAPSFunctions.{partitioned_id_assignment, rowIdSpaceBitsUsedByMonotonicallyIncreasingId}
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.sql.ddl.DdlParser.parse

class SqlPropertyGraphDataSourceTest extends CAPSTestSuite {

  private val dataSourceName = "fooDataSource"
  private val fooViewName = "fooView"
  private val fooGraphName = GraphName("fooGraph")

  private def computePartitionedRowId(rowIndex: Long, partitionStartDelta: Long): Long = {
    rowIndex + (partitionStartDelta << rowIdSpaceBitsUsedByMonotonicallyIncreasingId)
  }

  it("adds deltas to generated ids") {
    import sparkSession.implicits._
    val df = sparkSession.createDataFrame(Seq(Tuple1("A"), Tuple1("B"), Tuple1("C"))).toDF("alphabet")
    val withIds = df.withColumn("id", partitioned_id_assignment(0))
    val vanillaIds = List(0, 1, 2)
    withIds.select("id").collect().map(row => row.get(0)).toList should equal(vanillaIds)
    val idsWithDeltaAdded = df.withColumn("id", partitioned_id_assignment(2))
    val resultWithDelta = idsWithDeltaAdded.select("id").collect().map(row => row.get(0))
    resultWithDelta should equal(vanillaIds.map(computePartitionedRowId(_, 2)))
    resultWithDelta should equal(List(0x400000000L, 0x400000001L, 0x400000002L))

    val largeDf = sparkSession.sparkContext.parallelize(
      Seq.fill(100) {
        Tuple1("foo")
      }, 100
    ).toDF("fooCol")
    val largeDfWithIds = largeDf.withColumn("id", partitioned_id_assignment(100))
    val largeResultWithDelta = largeDfWithIds.select("id").collect().map(row => row.get(0).asInstanceOf[Long]).map(_ >> 33).sorted.toList
    val expectation = (0L until 100L).map(rowIndex => computePartitionedRowId(rowIndex, 100L + rowIndex)).map(_ >> 33).sorted.toList
    largeResultWithDelta should equal(expectation)
  }

  it("reads nodes from a table") {
    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.fooDatabaseName
         |
       |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Foo { foo : STRING })
         | (Foo)
         |
       |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Foo) FROM $fooViewName
         |  )
     """.stripMargin

    sparkSession.createDataFrame(Seq(Tuple1("Alice"))).toDF("foo").createOrReplaceTempView(fooViewName)

    val ds = SqlPropertyGraphDataSource(parse(ddlString), Map(dataSourceName -> SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Foo"), CypherMap("foo" -> "Alice")))
    ))
  }

  it("reads nodes from multiple tables") {
    val barViewName = "barView"
    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.fooDatabaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Foo { foo : STRING })
         | LABEL (Bar { bar : INTEGER })
         | (Foo)
         | (Bar)
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Foo) FROM $fooViewName
         |    (Bar) FROM $barViewName
         |  )
     """.stripMargin

    sparkSession.createDataFrame(Seq(Tuple1("Alice"))).toDF("foo").createOrReplaceTempView(fooViewName)
    sparkSession.createDataFrame(Seq(Tuple1(0L))).toDF("bar").createOrReplaceTempView(barViewName)

    val ds = SqlPropertyGraphDataSource(parse(ddlString), Map(dataSourceName -> SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0), Set("Foo"), CypherMap("foo" -> "Alice"))),
      CypherMap("n" -> CAPSNode(computePartitionedRowId(rowIndex = 0, partitionStartDelta = 1), Set("Bar"), CypherMap("bar" -> 0L)))
    ))
  }

}
