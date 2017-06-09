package org.opencypher.spark.impl.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.{TestSparkCypherSession, TestSuiteImpl}
import org.scalatest.mockito.MockitoSugar

class SparkCypherSessionImplTest
  extends TestSuiteImpl
    with TestSparkCypherSession.Fixture
    with MockitoSugar {

  import org.mockito.Mockito._

  test("refuse to import data frame from other session") {
    val df = mock[DataFrame]
    when(df.sparkSession).thenReturn(mock[SparkSession])

    an[IllegalArgumentException] shouldBe thrownBy(session.importDataFrame(df))
  }

  test("import a simple data frame and map it's types") {
    val df = sparkSession.createDataFrame(Seq(1 -> "a", 2 -> "b")).toDF("id", "name")

    val cf = session.importDataFrame(df)
    cf.columns should equal(Seq("id", "name"))
    cf.data should equal(df)
  }
}
