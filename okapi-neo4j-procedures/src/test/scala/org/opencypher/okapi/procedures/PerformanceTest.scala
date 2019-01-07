/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.procedures
import java.io.File

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.opencypher.okapi.impl.util.Measurement

object PerformanceTest extends App{

  val dbDirectory = "FILL ME"
  val graphDb = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(new File(dbDirectory))
    .setConfig(GraphDatabaseSettings.procedure_unrestricted, "*")
    .newGraphDatabase()

  registerShutdownHook(graphDb)

  val proceduresService = graphDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[Procedures])
  proceduresService.registerProcedure(classOf[OkapiProcedures], true)

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
