package org.opencypher.spark

import org.opencypher.spark_legacy.benchmark.RunBenchmark
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._

object PrototypeDemo {

  lazy val space = SparkGraphSpace.fromNeo4j("MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")(RunBenchmark.sparkSession)

  def cypher(query: String): SparkCypherGraph = {
    println(s"Now executing query: $query")

    val result: SparkCypherResult = space.base.cypher(query)

    val start = System.currentTimeMillis()
    result.records.toDF().count()
    println(s"Time: ${System.currentTimeMillis() - start} ms")

    result.graph
  }

  def main(args: Array[String]): Unit = {
    cypher("MATCH (t:User)-[:ATTENDED]->() WHERE t.country = 'ca' RETURN t.city, t.id")
  }
}
