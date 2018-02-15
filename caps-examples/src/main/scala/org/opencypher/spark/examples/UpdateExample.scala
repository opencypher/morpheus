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

import org.apache.spark.sql.Dataset
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.CAPSSession._
import org.opencypher.caps.api.value.CAPSNode
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.spark.encoders._

import scala.collection.JavaConverters._

/**
  * Demonstrates how to retrieve Cypher entities as a Dataset and update them.
  */
object UpdateExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (p:Person)
       |WHERE p.age >= 18
       |RETURN p""".stripMargin)

  // 4) Extract Dataset representing the query result
  val ds = results.records.asDataset

  // 5) Add a new label and property to the nodes
  val adults: Dataset[CAPSNode] = ds.map { record: CypherMap =>
    record("p").cast[CAPSNode].withLabel("Adult").withProperty("canVote", true)
  }

  // 6) Print updated nodes
  println(adults.toLocalIterator.asScala.toList.mkString("\n"))

}
