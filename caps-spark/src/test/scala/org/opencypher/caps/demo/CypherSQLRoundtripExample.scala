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
package org.opencypher.caps.demo

import org.opencypher.caps.api.{CAPSSession, schema}
import org.opencypher.caps.impl.record.CypherRecords

/**
  * Demonstrates usage patterns where Cypher and SQL can be interleaved in the
  * same processing chain, by using the tabular output of a Cypher query as a
  * SQL table, and using the output of a SQL query as an input driving table
  * for a Cypher query.
  */
object CypherSQLRoundtripExample extends App {
  // 1) Create CAPS session
  implicit val session = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SampleData.persons, SampleData.friendships)

  // 3) Query for a view of the people in the social network
  val result = socialNetwork.cypher(
    """|MATCH (p:Person)
       |RETURN p.age AS age, p.name AS name
    """.stripMargin
  )

  // 4) Register the result as a table called people
  result.records.register("people")

  // 5) Query the registered table using SQL
  val sqlResults: CypherRecords = session.sql("SELECT age, name FROM people")

  sqlResults.print

  // 6) Load a purchase network graph via CSV + Schema files
//  val csvFolder = getClass.getResource("/csv/prod/").getFile
//  val purchaseNetwork = session.readFrom(s"file+csv://$csvFolder")

  // 7) Use the results from the SQL query as driving table for a Cypher query
//  val result2 = purchaseNetwork.cypher("MATCH (c:Customer {name: name})-->(p:Product) RETURN c.name, age, p.title")//,
//    drivingTable = Some(sqlRecords))

//  result2.print

}

/**
  * Specify schema and data with case classes.
  */
object SampleData {

  case class Person(id: Long, name: String, age: Int) extends schema.Node

  @schema.RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends schema.Relationship

  val alice = Person(0, "Alice", 10)
  val bob = Person(1, "Bob", 15)
  val carol = Person(2, "Carol", 25)

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
