package org.opencypher.caps.api.spark

import java.net.URI

import org.opencypher.caps.CAPSTestSuiteWithHDFS
import org.opencypher.caps.api.io.hdfs.HdfsCsvGraphSource
import org.opencypher.caps.api.io.neo4j.Neo4JGraphSource

class CAPSSessionTest extends CAPSTestSuiteWithHDFS {

  test("HDFS via factory") {
    val graph = CAPSSession.builder(session).get.withGraphAt(hdfsURI, "temp")
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }

  test("HDFS via mount") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/test/graph", HdfsCsvGraphSource(sparkSession.sparkContext.hadoopConfiguration, hdfsURI.getPath))
      .get

    val graph = capsSession.withGraphAt(URI.create("/test/graph"), "test")
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }

  ignore("Neo4j via factory") {
    val capsSession = CAPSSession.builder(session).get

    val result = capsSession
      .withGraphAt(URI.create("bolt://user:password@localhost:1234?MATCH%20(n)%20RETURN%20n"), "allNodes")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }


  ignore("Neo4j via mount") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/neo4j1", Neo4JGraphSource(URI.create("bolt://my-ip:1234"), "alice", "secret", "MATCH (n) RETURN n"))
      .withGraphSource("/neo4j2", Neo4JGraphSource(URI.create("bolt://your-ip:1234"), "bob", "secret", "MATCH (n) RETURN n"))
      .get

    val result = capsSession
      .withGraphAt(URI.create("/neo4j1"), "allNodes")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }

  ignore("preconfigured Neo4j via url") {
    val capsSession = CAPSSession
      .builder(session)
      .withGraphSource(Neo4JGraphSource(URI.create("bolt://my.neo4j.com"), "anonymous", "passwd", "MATCH ..."))
      .get

    val result = capsSession
      .withGraphAt(URI.create("bolt://my.neo4j.com"), "socnet")
      .cypher("MATCH (p:Person) RETURN p")

    result.showRecords()
  }
}
