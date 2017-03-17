package org.opencypher.spark.prototype

import org.opencypher.spark.benchmark.RunBenchmark
import org.opencypher.spark.prototype.api.spark.SparkGraphSpace
import org.opencypher.spark.prototype.impl.instances.spark.cypher._
import org.opencypher.spark.prototype.impl.syntax.cypher._

object PrototypeDemo2 {

  val space = SparkGraphSpace.fromNeo4j("MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")(RunBenchmark.sparkSession)

  def cypher(query: String) = {
    println(s"Now executing query: $query")

    val graph = space.base.cypher(query)

    val start = System.currentTimeMillis()
    graph.records.show()
    println(s"Time: ${System.currentTimeMillis() - start} ms")

    graph
  }

  def main(args: Array[String]): Unit = {
    cypher("MATCH (t:User)-[:ATTENDED]->() WHERE t.country = 'ca' RETURN t.city, t.id")
  }
}
