package org.opencypher.spark.prototype

import org.opencypher.spark.api.CypherResultContainer
import org.opencypher.spark.benchmark.RunBenchmark

import scala.io.StdIn

object PrototypeDemo {

  lazy val prototype = new Prototype {
    override val graph = RunBenchmark.createStdPropertyGraphFromNeo(-1)
  }

  def cypher(query: String): CypherResultContainer = {
    val result = prototype.cypher(query)

    val start = System.currentTimeMillis()
    result.show()
    println(s"Time: ${System.currentTimeMillis() - start} ms")

    result
  }

  def main(args: Array[String]): Unit = {

    // cache graph up front
    prototype.cypher("MATCH (a) RETURN a.name")

    while (true) {
      println("Type your query:\n")
      val query = StdIn.readLine()

      prototype.cypher(query).show()
    }
  }

  /*
  * MATCH (a:Answer)-->(b:Question) RETURN b.title, b.is_answered
  */
}
