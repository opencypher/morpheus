package org.opencypher.testing.services

import org.neo4j.harness.TestServerBuilders

object Neo4j extends App {
  val marker = args.headOption
  val server = TestServerBuilders.newInProcessBuilder()
    .withConfig("dbms.connector.bolt.listen_address", "127.0.0.1:7687")
    .withConfig("dbms.connector.http.listen_address", "127.0.0.1:7474")
    .newServer()

  println(s"bolt: ${server.boltURI()}")
  println(s"http: ${server.httpURI()}")
  marker.foreach(println)

}
