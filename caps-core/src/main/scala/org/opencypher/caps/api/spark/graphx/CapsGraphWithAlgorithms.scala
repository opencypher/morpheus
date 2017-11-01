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
package org.opencypher.caps.api.spark.graphx

import org.apache.spark.graphx._
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.opencypher.caps.api.spark.CAPSGraph

object Algorithms {

  implicit class CapsGraphWithAlgorithms(graph: CAPSGraph) {
    def toGraphX(implicit sparkSession: SparkSession): Graph[String, String] = {
      import sparkSession.implicits._

      val nodeRecords = graph.nodes("n")
      val nodes = nodeRecords.data.toDF()
      implicit val nodeEncoder = Encoders.tuple[Long, Row](Encoders.scalaLong, nodeRecords.header.rowEncoder)
      val nodesInGraphXFormat = nodes.map { n =>
        val idIndex = n.fieldIndex("n")
        (n.get(idIndex).asInstanceOf[VertexId], "")
      }.rdd

      val relRecords = graph.relationships("r")
      val relationships = relRecords.data.toDF()
      implicit val edgeEncoder = Encoders.product[Edge[String]]
      val relsInGraphXFormat = relationships.map {
        r =>
          val sourceIndex = r.fieldIndex("____source(r)")
          val targetIndex = r.fieldIndex("____target(r)")
          val relTypeIndex = r.fieldIndex("____type(r)")
          Edge(r.get(sourceIndex).asInstanceOf[VertexId], r.get(targetIndex).asInstanceOf[VertexId], r.get(relTypeIndex).asInstanceOf[String])
      }.rdd
      val graphX = Graph[String, String](nodesInGraphXFormat, relsInGraphXFormat)
      graphX
    }

  }

}
