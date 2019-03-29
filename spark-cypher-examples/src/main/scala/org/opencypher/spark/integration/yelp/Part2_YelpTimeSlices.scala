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
package org.opencypher.spark.integration.yelp

import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants.{defaultYelpGraphFolder, yelpGraphName, _}

object Part2_YelpTimeSlices extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)

  // Construct Phoenix sub graph
  cypher(
    s"""
       |CATALOG CREATE GRAPH $phoenixGraphName {
       |  FROM GRAPH $fsNamespace.$yelpGraphName
       |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
       |  WHERE b.city = 'Phoenix'
       |  CONSTRUCT
       |   CREATE (user)-[r]->(b)
       |  RETURN GRAPH
       |}
      """.stripMargin)

  // Slice graph into buckets and store in file system
  (2015 to 2018) foreach { year =>
    cypher(
      s"""
         |CATALOG CREATE GRAPH $fsNamespace.year$year {
         |  FROM GRAPH $phoenixGraphName
         |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
         |  WHERE r.date.year = $year
         |  CONSTRUCT
         |   CREATE (user)-[r]->(b)
         |  RETURN GRAPH
         |}
     """.stripMargin)
  }
}
