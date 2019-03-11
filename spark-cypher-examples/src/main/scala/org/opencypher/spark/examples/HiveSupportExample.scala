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
// tag::full-example[]
package org.opencypher.spark.examples

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.{GraphName, Node}
import org.opencypher.spark.api.io.util.HiveTableName
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.App

object HiveSupportExample extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .enableHiveSupport()
    .appName(s"caps-local-${UUID.randomUUID()}")
    .getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")
  implicit val session = CAPSSession.create(sparkSession)

  val hiveDatabaseName = "socialNetwork"
  session.sparkSession.sql(s"DROP DATABASE IF EXISTS $hiveDatabaseName CASCADE")
  session.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDatabaseName")


  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
  val tmp = s"file:///${System.getProperty("java.io.tmpdir").replace("\\", "/")}/${System.currentTimeMillis()}"

  val fs = GraphSources.fs(tmp, Some(hiveDatabaseName)).parquet
  val graphName = GraphName("sn")

  fs.store(graphName, socialNetwork)

  val nodeTableName = HiveTableName(hiveDatabaseName, graphName, Node, Set("Person"))

  val result = session.sql(s"SELECT * FROM $nodeTableName WHERE property_age >= 15")

  result.show

  fs.delete(graphName)
  session.sparkSession.sql(s"DROP DATABASE IF EXISTS $hiveDatabaseName CASCADE")
}
// end::full-example[]
