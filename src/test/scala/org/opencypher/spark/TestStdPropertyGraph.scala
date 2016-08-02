package org.opencypher.spark

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset}
import org.opencypher.spark.impl.StdPropertyGraph

case class TestStdPropertyGraph(nodes: Dataset[CypherNode], relationships: Dataset[CypherRelationship])(val sqlContext: SQLContext) extends StdPropertyGraph(sqlContext)

object TestPropertyGraphs {

  import CypherValue.implicits._

  val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
  val sc = session.sqlContext

  def graph1: TestStdPropertyGraph = {
    val nodes = sc.createDataset(Seq(CypherNode("prop" -> CypherString("value")), CypherNode("Label"), CypherNode("Label1", "Label2"), CypherNode("name" -> CypherString("foo"))))
    val relationships = sc.createDataset(Seq(CypherRelationship(0, 1, "KNOWS")))

    TestStdPropertyGraph(nodes, relationships)(sc)
  }
}


