/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.examples

import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.CAPSSession._

/**
  * Shows how to access a Cypher query result as a [[DataFrame]].
  */
object DataFrameOutputExample extends App {

  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since""".stripMargin)

  // 4) Extract DataFrame representing the query result
  val df: DataFrame = results.records.asDataFrame

  // 5) Select specific return items from the query result
  val projection: DataFrame = df.select(columnFor("a.name"), columnFor("b.name"))

  projection.show()
}

/**
  * Alternative to accessing a Cypher query result as a [[DataFrame]].
  */
object DataFrameOutputUsingAliasExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name AS person1, b.name AS person2, r.since AS friendsSince""".stripMargin)

  // 4) Extract DataFrame representing the query result
  val df: DataFrame = results.records.asDataFrame

  // 5) Select aliased return items from the query result
  val projection: DataFrame = df
    .select("person1", "friendsSince", "person2")
    .orderBy(functions.to_date(df.col("friendsSince"), "dd/mm/yyyy"))

  projection.show()
}
