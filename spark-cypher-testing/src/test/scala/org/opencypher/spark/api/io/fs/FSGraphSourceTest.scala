package org.opencypher.spark.api.io.fs

import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.{GraphName, Node, Relationship}
import org.opencypher.spark.api.io.ParquetFormat
import org.opencypher.spark.api.io.util.HiveTableName
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.testing.CAPSTestSuite

class FSGraphSourceTest extends CAPSTestSuite with DefaultGraphInit {

  private var tempDir = new TemporaryFolder()

  private val testDatabaseName = "test"

  override protected def beforeEach(): Unit = {
    caps.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $testDatabaseName")
    tempDir.create()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    caps.sparkSession.sql(s"DROP DATABASE IF EXISTS $testDatabaseName CASCADE")
    tempDir.delete()
    tempDir = new TemporaryFolder()
    super.afterEach()
  }

  describe("Hive support") {

    val graphName = GraphName("foo")
    val nodeTableName = HiveTableName(testDatabaseName, graphName, Node, Set("L"))
    val relTableName = HiveTableName(testDatabaseName, graphName, Relationship, Set("R"))
    val testGraph = initGraph("CREATE (:L {prop: 'a'})-[:R {prop: 'b'}]->(:L {prop: 'c'})")

    it("writes nodes and relationships to hive tables") {
      val given = testGraph

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"), ParquetFormat, Some(testDatabaseName), None)
      fs.store(graphName, given)

      val nodeResult = caps.sparkSession.sql(s"SELECT * FROM $nodeTableName")
      nodeResult.collect().toSet should equal(
        Set(
          Row(1, "c"),
          Row(0, "a")
        )
      )

      val relResult = caps.sparkSession.sql(s"SELECT * FROM $relTableName")
      relResult.collect().toSet should equal(
        Set(
          Row(2, 0, 1, "b")
        )
      )
    }

    it("deletes the hive database if the graph is deleted") {
      val given = testGraph

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"), ParquetFormat, Some(testDatabaseName), None)
      fs.store(graphName, given)

      caps.sparkSession.sql(s"SELECT * FROM $nodeTableName").collect().toSet should not be empty
      caps.sparkSession.sql(s"SELECT * FROM $relTableName").collect().toSet should not be empty

      fs.delete(graphName)
      an [AnalysisException] shouldBe thrownBy {
        caps.sparkSession.sql(s"SELECT * FROM $nodeTableName")
      }
      an [AnalysisException] shouldBe thrownBy {
        caps.sparkSession.sql(s"SELECT * FROM $relTableName")
      }
    }

  }

}
