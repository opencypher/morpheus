package org.opencypher.morpheus

import org.apache.spark.graph.api.{NodeDataset, RelationshipDataset}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GraphApp extends App {
  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  //  implicit val cypherSession = SparkCypherSession.create
  implicit val cypherSession: MorpheusCypherSession = MorpheusCypherSession.create

  // --------
  // SPIP API
  // --------

  val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
  val nodeFrame: NodeDataset = NodeDataset.builder(nodeData)
    .idColumn("id")
    .labelSet(Array("Person"))
    .properties(Map("name" -> "name"))
    .build()

  val graph = cypherSession.createGraph(Array(nodeFrame), Array.empty[RelationshipDataset])
  val result = graph.cypher("MATCH (n) RETURN n")
  result.ds.show()

  // ------------
  // Morpheus API
  // ------------

  // MorpheusSession needs to be in implicit scope for PGDSs etc.

  import org.opencypher.morpheus.api.GraphSources
  import org.opencypher.okapi.api.graph.Namespace
  implicit val morpheus = cypherSession.morpheus

  cypherSession.registerSource(Namespace("fs"), GraphSources.fs(getClass.getResource("/csv").getFile).csv)
  cypherSession.cypher(s"FROM GRAPH fs.products MATCH (n) RETURN n").show
  cypherSession.cypher(
    s"""
       |FROM GRAPH fs.products
       |MATCH (n)
       |CONSTRUCT
       | CREATE (n)
       |RETURN GRAPH""".stripMargin).graph.nodes("n").show
}
