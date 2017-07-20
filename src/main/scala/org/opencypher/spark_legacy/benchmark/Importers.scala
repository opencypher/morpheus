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
package org.opencypher.spark_legacy.benchmark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.spark.Neo4j

object Importers {

  val neo4j = Neo4j(RunBenchmark.sparkSession.sparkContext)

  def importFromNeo(size: Long): (RDD[InternalNode], RDD[InternalRelationship]) = {
    if (size > 0) limit(neo4j, size)
    else all(neo4j)
  }

  def importMusicBrainz(size: Long) = {
    import scala.reflect.classTag

    val labels: Array[String] = neo4j.cypher("call db.labels").loadRdd(classTag[String]).collect()

    val nodeMap = labels.map { l =>
      l -> neo4j.cypher(s"MATCH (n:$l) RETURN n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    }.toMap

    val labelFields = labels.map { l =>
      StructField(l.toLowerCase, BooleanType)
    }
    val nodeFields = StructField("id", LongType) +: labelFields
    val nodeSchema = StructType(nodeFields)

    val frameMap = nodeMap.mapValues { rdd =>
      val rowRdd = rdd.map { node =>
        val fields = node.id() +: labels.map(node.hasLabel)
        Row(fields: _*)
      }
      RunBenchmark.sparkSession.createDataFrame(rowRdd, nodeSchema)
    }
    val types: Array[String] = neo4j.cypher("call db.relationshipTypes").loadRdd(classTag[String]).collect()

    val tripletMap = types.map { t =>
      t -> {
        neo4j.cypher(s"MATCH ()-[r:$t]->(e) RETURN r, e").loadRowRdd.map { row =>
          row(0).asInstanceOf[InternalRelationship] -> row(1).asInstanceOf[InternalNode]
        }
      }
    }.toMap

    val tripletFields = Array(StructField("id", LongType), StructField("startId", LongType), StructField("endId", LongType)) ++ labelFields
    val tripletSchema = StructType(tripletFields)

    val tripletFrames = tripletMap.mapValues { rdd =>
      val rowRdd = rdd.map {
        case (rel, node) =>
          val fields = Array(rel.id(), rel.startNodeId(), rel.endNodeId()) ++ labels.map(node.hasLabel)
          Row(fields: _*)
      }
      RunBenchmark.sparkSession.createDataFrame(rowRdd, tripletSchema)
    }

    frameMap -> tripletFrames
  }

  private def limit(neo4j: Neo4j, limit: Long) = {
    println(s"Importing $limit relationships from Neo4j")
    val nodes = neo4j.cypher(s"MATCH (b)-[r:MEMBER_OF]->(a) WITH a, b LIMIT $limit UNWIND [a, b] AS n RETURN DISTINCT n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])

    val rels = neo4j.cypher(s"MATCH ()-[r:MEMBER_OF]->() RETURN r LIMIT $limit").loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

  private def all(neo4j: Neo4j) = {
    println(s"Importing ALL nodes and relationships from Neo4j")
    val nodes = neo4j.cypher("CYPHER runtime=compiled MATCH (n) RETURN n").loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])

    val rels = neo4j.cypher("CYPHER runtime=compiled MATCH ()-[r]->() RETURN r").loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

}
