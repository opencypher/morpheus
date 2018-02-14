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

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.graphx._
import org.apache.spark.util.collection.{BitSet, OpenHashSet}
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.CAPSSession._
import org.opencypher.caps.impl.spark.CypherKryoRegistrator

object PageRankExample extends App {

  // 1) Create CAPS session
  implicit val session = CAPSSession.local("spark.kryo.registrator" -> classOf[CustomKryoRegistrator].getCanonicalName)

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val nodes = socialNetwork.cypher(
    """|MATCH (n:Person)
       |RETURN id(n), n.name""".stripMargin)

  val rels = socialNetwork.cypher(
    """|MATCH (:Person)-[r]->(:Person)
       |RETURN startNode(r), endNode(r)
    """.stripMargin
  )

  // 4) Create GraphX compatible RDDs from nodes and relationships
  val graphXNodeRDD = nodes.records.asDF.rdd.map(row => (row.getLong(0), row.getString(1)))
  val graphXRelRDD = rels.records.asDF.rdd.map(row => Edge(row.getLong(0), row.getLong(1), ()))

  // 5) Compute Page Rank via GraphX
  val graph = Graph(graphXNodeRDD, graphXRelRDD)
  val ranks = graph.pageRank(0.0001).vertices.join(graphXNodeRDD).map { case (_, (rank, name)) => name -> rank }

  // 6) Sort and print resulting ranks
  val rankDF = session.sparkSession.createDataFrame(ranks)
    .withColumnRenamed("_1", "name")
    .withColumnRenamed("_2", "rank")

  rankDF.orderBy(rankDF.col("rank").desc).show
}

/**
  * Example for a Kryo registrator that contains application specific class registrations.
  */
class CustomKryoRegistrator extends CypherKryoRegistrator {

  // GraphX
  val graphXClasses: Seq[Class[_]] = Seq(
    classOf[Edge[Unit]],
    classOf[(VertexId, Unit)],
    Class.forName("org.apache.spark.graphx.impl.EdgePartition"),
    classOf[BitSet],
    Class.forName("org.apache.spark.graphx.impl.VertexAttributeBlock"),
    classOf[PartitionStrategy],
    classOf[EdgeDirection],
    classOf[OpenHashSet[Int]],
    classOf[OpenHashSet[Long]])

  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)

    import com.twitter.chill.toRich
    kryo.registerClasses(graphXClasses)
  }
}