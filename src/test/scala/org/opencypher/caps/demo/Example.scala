/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.caps.api.record._
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}

case class Person(id: Long, name: String) extends Node

case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

object Example extends App {
  // Node and relationship data
  val persons = List(Person(0, "Alice"), Person(1, "Bob"), Person(2, "Carol"))
  val friendships = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))

  // Create CAPS session
  implicit val caps = CAPSSession.local()

  // Create graph from nodes and relationships
  val graph = CAPSGraph.create(persons, friendships)

  // Query graph with Cypher
  val results = graph.cypher(
    """| MATCH (a:Person)-[r:FRIEND]->(b)
       | RETURN a.name AS person, b.name AS friendsWith, r.since AS since""".stripMargin
  )

  case class ResultSchema(person: String, friendsWith: String, since: String)

  // Print result rows mapped to a case class
  results.as[ResultSchema].foreach(println)
}
