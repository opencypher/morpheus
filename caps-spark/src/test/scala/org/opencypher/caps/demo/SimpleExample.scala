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

import org.opencypher.caps.api._

/**
  * Demonstrates basic usage of the CAPS API by loading an example network via case class instances and issuing a query
  * on the graph.
  */
object SimpleExample extends App {

  // 1) Create CAPS session
  implicit val session = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since""".stripMargin
  )

  // 4) Print results to console
  results.print
}

/**
  * Specify schema and data via case classes
  */
object SocialNetworkData {

  case class Person(id: Long, name: String) extends schema.Node

  @schema.RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends schema.Relationship

  val alice = Person(0, "Alice")
  val bob = Person(1, "Bob")
  val carol = Person(2, "Carol")

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
