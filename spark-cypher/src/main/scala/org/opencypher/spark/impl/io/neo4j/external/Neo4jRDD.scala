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
package org.opencypher.spark.impl.io.neo4j.external

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

private class Neo4jRDD(
    sc: SparkContext,
    val query: String,
    val neo4jConfig: Neo4jConfig,
    val parameters: Map[String, Any] = Map.empty,
    partitions: Partitions = Partitions())
    extends RDD[Row](sc, Nil) {

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {

    val neo4jPartition: Neo4jPartition = partition.asInstanceOf[Neo4jPartition]

    Executor.execute(neo4jConfig, query, parameters ++ neo4jPartition.window).sparkRows
  }

  override protected def getPartitions: Array[Partition] = {
    val p = partitions.effective()
    Range(0, p.partitions.toInt).map(idx => new Neo4jPartition(idx, p.skip(idx), p.limit(idx))).toArray
  }

  override def toString(): String = s"Neo4jRDD partitions $partitions $query using $parameters"
}
