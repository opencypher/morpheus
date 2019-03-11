package org.opencypher.testing.services

import org.neo4j.harness.TestServerBuilders

object Neo4j extends App {
  val Array(marker, instances) = args

  Range(0, instances.toInt).foreach { i =>
    val bolt = 7687 + i
    val http = 7474 + i
    val server = TestServerBuilders.newInProcessBuilder()
      .withConfig("dbms.connector.bolt.listen_address", s"127.0.0.1:$bolt")
      .withConfig("dbms.connector.http.listen_address", s"127.0.0.1:$http")
      .newServer()

    println(s"bolt: ${server.boltURI()}")
    println(s"http: ${server.httpURI()}")
  }

  println(marker)

}
