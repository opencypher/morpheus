package org.opencypher.spark.benchmark

import org.neo4j.driver.v1.{Driver, Session, Value}
import org.opencypher.spark.impl.SupportedQuery

object Neo4jViaDriverBenchmarks extends SupportedQueryBenchmarks[Neo4jViaDriverGraph] {

  def apply(query: SupportedQuery): Benchmark[Neo4jViaDriverGraph] =
    new Neo4jViaDriverBenchmark(query.toString)
}

class Neo4jViaDriverBenchmark(query: String) extends Benchmark[Neo4jViaDriverGraph] {

  override def name: String = "Neo4j     "

  override def run(graph: Neo4jViaDriverGraph): Outcome = {
    graph.withSession { session =>
      val intType = session.typeSystem().INTEGER()
      var count = 0
      var checksum = 0
      val result = session.run(query)

      while (result.hasNext) {
        val record = result.next()
        count += 1
        val value = record.get(0)
        val increment = if (value.hasType(intType)) value.asLong() else if (value.isNull) -1L else value.asEntity().id()
        checksum ^= increment.hashCode()
      }
      val summary = result.consume()

      new Outcome {
        override lazy val plan = graph.withSession(session => session.run(s"EXPLAIN $query").consume().plan().toString)
        override val computeCount: Long = count
        override val computeChecksum: Int = checksum
      }
    }
  }

  def numNodes(graph: Neo4jViaDriverGraph): Long =
    graph.withSession(_.run("MATCH (n) RETURN count(n)").single().get(0).asLong())

  def numRelationships(graph: Neo4jViaDriverGraph): Long =
    graph.withSession(_.run("MATCH ()-[r]->() RETURN count(r)").single().get(0).asLong())
}

class Neo4jViaDriverGraph(driver: Driver) {
  def withSession[T](f: Session => T): T = {
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }
}

