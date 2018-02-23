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

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.io.GraphName
import org.opencypher.caps.impl.spark.io.file.FileCsvPropertyGraphDataSource

/**
  * Demonstrates usage patterns where Cypher and SQL can be interleaved in the
  * same processing chain, by using the tabular output of a Cypher query as a
  * SQL table, and using the output of a SQL query as an input driving table
  * for a Cypher query.
  */
object CypherSQLRoundtripExample extends App {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query for a view of the people in the social network
  val result = socialNetwork.cypher(
    """|MATCH (p:Person)
       |RETURN p.age AS age, p.name AS name
    """.stripMargin
  )

  // 4) Register the result as a table called people
  result.records.register("people")

  // 5) Query the registered table using SQL
  val sqlResults = session.sql("SELECT age, name FROM people")

  // 6) Load a purchase network graph via CSV + Schema files
  val csvFolder = getClass.getResource("/csv").getFile
  val purchaseNetwork = new FileCsvPropertyGraphDataSource(rootPath = csvFolder).graph(GraphName.from("prod"))

  // 7) Use the results from the SQL query as driving table for a Cypher query
  val result2 = purchaseNetwork.cypher("WITH name AS name, age AS age MATCH (c:Customer {name: name})-->(p:Product) RETURN c.name, age, p.title", drivingTable = Some(sqlResults))

  result2.print
}
