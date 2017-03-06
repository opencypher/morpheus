package org.opencypher.spark.prototype

import org.opencypher.spark.benchmark.RunBenchmark

import scala.io.StdIn

object PrototypeDemo {
  def main(args: Array[String]): Unit = {
    val cosPrototype = new Prototype {
      override val graph = RunBenchmark.createStdPropertyGraphFromNeo(1000)
    }

    // cache graph up front
    cosPrototype.cypher("MATCH (a) RETURN a.name")

    while (true) {
      println("Type your query:\n")
      val query = StdIn.readLine()

      cosPrototype.cypher(query).show()
    }
  }
}
