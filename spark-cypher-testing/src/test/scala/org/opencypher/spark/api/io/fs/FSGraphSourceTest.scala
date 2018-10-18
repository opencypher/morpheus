package org.opencypher.spark.api.io.fs

import org.apache.spark.sql.Row
import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.ParquetFormat
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.testing.CAPSTestSuite

class FSGraphSourceTest extends CAPSTestSuite with DefaultGraphInit {

  private var tempDir = new TemporaryFolder()

  override protected def beforeEach(): Unit = {
    caps.sparkSession.sql("CREATE DATABASE IF NOT EXISTS test")
    tempDir.create()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    caps.sparkSession.sql("DROP DATABASE test CASCADE")
    tempDir.delete()
    tempDir = new TemporaryFolder()
    super.afterEach()
  }

  describe("Hive support") {

    it("writes nodes to hive tables") {
      val given = initGraph("CREATE (:L {prop: 'a'}), (:L {prop: 'c'})")

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"), ParquetFormat, None, Some("test"))
      fs.store(GraphName("foo"), given)

      val result = caps.sparkSession.sql("SELECT * FROM test.foo_nodes_L")
      result.collect().toSet should equal(
        Set(
          Row(1, "c"),
          Row(0, "a")
        )
      )
    }

    it("writes relationships to hive tables") {
      val given = initGraph("CREATE (:L {prop: 'a'})-[:R {prop: 'b'}]->(:L {prop: 'c'})")

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"), ParquetFormat, None, Some("test"))
      fs.store(GraphName("foo"), given)

      val result = caps.sparkSession.sql("SELECT * FROM test.foo_relationships_R")
      result.collect().toSet should equal(
        Set(
          Row(2, 0, 1, "b")
        )
      )
    }

  }

}
