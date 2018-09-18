package org.opencypher.okapi.neo4j.io

import java.io.File

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.opencypher.okapi.impl.util.Measurement

object PerformanceTest extends App {

  val dbDirectory = "FILL ME"
  val graphDb = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(new File(dbDirectory))
    .setConfig(GraphDatabaseSettings.procedure_unrestricted, "*")
    .newGraphDatabase()

  registerShutdownHook(graphDb)

  println("Neo4j startup complete")

  Measurement.printTiming("Initial Procedure run") {
    val tx = graphDb.beginTx()
    val res = graphDb.execute("CALL org.opencypher.okapi.procedures.schema")
    println(res.resultAsString)
    tx.close()
  }

  Measurement.printTiming("Second Procedure run") {
    val tx = graphDb.beginTx()
    val res = graphDb.execute("CALL org.opencypher.okapi.procedures.schema")
    println(res.resultAsString)
    tx.close()
  }

  graphDb.shutdown()

  private def registerShutdownHook(graphDb: GraphDatabaseService): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        graphDb.shutdown()
      }
    })
  }
}
