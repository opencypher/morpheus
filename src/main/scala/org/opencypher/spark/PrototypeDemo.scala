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
package org.opencypher.spark

import org.opencypher.spark_legacy.benchmark.RunBenchmark
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._

object PrototypeDemo {

  lazy val space = SparkGraphSpace.fromNeo4j("MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")(RunBenchmark.sparkSession)

  def cypher(query: String): SparkCypherResult = {
    println(s"Now executing query: $query")

    val result: SparkCypherResult = space.base.cypher(query)

    val start = System.currentTimeMillis()
    result.records.toDF().count()
    println(s"Time: ${System.currentTimeMillis() - start} ms")

    result
  }

  def main(args: Array[String]): Unit = {
    cypher("MATCH (t:User)-[:ATTENDED]->() WHERE t.country = 'ca' RETURN t.city, t.id")
  }
}
