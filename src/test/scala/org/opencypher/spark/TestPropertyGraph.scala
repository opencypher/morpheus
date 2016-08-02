package org.opencypher.spark

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset}

case class TestPropertyGraph(nodes: Dataset[CypherNode], relationships: Dataset[CypherRelationship])(val sqlContext: SQLContext) extends PropertyGraphImpl(sqlContext)


object TestPropertyGraphs extends CypherEncoders {

  val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
  val sc = session.sqlContext

  def graph1: TestPropertyGraph = {
    val nodes = sc.createDataset(Seq(CypherNode("prop" -> CypherString("value")), CypherNode("Label"), CypherNode("Label1", "Label2"), CypherNode("name" -> CypherString("foo"))))
    val relationships = sc.createDataset(Seq(CypherRelationship(0, 1, "KNOWS")))

    TestPropertyGraph(nodes, relationships)(sc)
  }



}


