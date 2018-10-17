package org.opencypher.spark.api.io.sql

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.HiveFormat
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.sql.ddl.DdlParser.parse

class SqlPropertyGraphDataSourceTest extends CAPSTestSuite {

  private val dataSourceName = "fooDataSource"
  private val fooViewName = "fooView2"
  private val fooGraphName = GraphName("fooGraph")

  private val ddlString =
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

  it("reads nodes from a hive table") {
    sparkSession.sql("DROP TABLE IF EXISTS " + fooViewName)
    sparkSession.createDataFrame(Seq(Tuple1("Alice")))
      .toDF( "foo")
      .write.saveAsTable(fooViewName)

    val ddl = parse(ddlString)
    val ds = SqlPropertyGraphDataSource(ddl, Map(dataSourceName -> SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    val graph = ds.graph(fooGraphName)

    graph.nodes("n").show
  }

}
