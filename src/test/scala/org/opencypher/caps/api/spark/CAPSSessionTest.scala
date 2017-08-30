package org.opencypher.caps.api.spark

import java.net.URI

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.io.hdfs.HdfsCsvGraphSource
import org.opencypher.caps.api.io.neo4j.Neo4JGraphSource

class CAPSSessionTest extends CAPSTestSuite {

  test("neo4j via factory") {
    val capsSession = CAPSSession.builder(session).get

    val result = capsSession
      .withGraphAt(URI.create("bolt://user:password@localhost:1234?MATCH%20(n)%20RETURN%20n"), "allNodes")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }

  test("neo4j via mount") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/neo4j1", Neo4JGraphSource(URI.create("bolt://my-ip:1234"), "alice", "secret", "MATCH (n) RETURN n"))
      .withGraphSource("/neo4j2", Neo4JGraphSource(URI.create("bolt://your-ip:1234"), "bob", "secret", "MATCH (n) RETURN n"))
      .get

    val result = capsSession
      .withGraphAt(URI.create("/neo4j1"), "allNodes")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }

  test("hdfs via factory") {
    val capsSession = CAPSSession.builder(session).get

    val result = capsSession
      .withGraphAt(URI.create("hdfs+csv://localhost:1234/data/fb/graph/2016/05"), "socnet")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }

  test("hdfs via mount") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/socnet/june", HdfsCsvGraphSource(sparkSession.sparkContext.hadoopConfiguration, "/data/fb/graph/2016/06"))
      .withGraphSource("/socnet/july", HdfsCsvGraphSource(sparkSession.sparkContext.hadoopConfiguration, "/data/fb/graph/2016/07"))
      .get

    val result = capsSession
      .withGraphAt(URI.create("/socnet/june"), "socnet")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }
}
