/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.examples

import org.opencypher.okapi.api.configuration.Configuration.PrintDebug
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.io.sql.IdGenerationStrategy
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.{ConsoleApp, HiveSetupForDebug}

object BugFinding extends ConsoleApp {

  PrintDebug.set()

  implicit val session: CAPSSession = CAPSSession.local()

  // Load a CSV file of interactions into Hive tables (views)
  HiveSetupForDebug.load()

  session.registerSource(Namespace("sql"), GraphSources
    .sql(file("/customer-interactions/ddl/debug.ddl"))
//    .withIdGenerationStrategy(IdGenerationStrategy.HashBasedId)
    .withSqlDataSourceConfigs(file("/customer-interactions/ddl/data-sources.json")))

  val c360Seed = session.cypher(
    """
      |FROM sql.debug
      |RETURN GRAPH
    """.stripMargin).graph

  c360Seed.cypher(
    """
      |MATCH ()
      |RETURN count(*) AS `nodeCount (26 is correct)`
    """.stripMargin).show

  c360Seed.cypher(
    """
      |MATCH ()-->()
      |RETURN count(*) AS `relCount (36 is correct)`
    """.stripMargin).show

  session.sparkSession.close()


  def file(path: String): String = {
    getClass.getResource(path).toURI.getPath
  }
}
