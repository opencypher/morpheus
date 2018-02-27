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
package org.opencypher.spark.demo

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.{CypherResult, GraphName}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.SparkConfiguration.MasterAddress
import org.opencypher.spark.api.io.file.FileCsvPropertyGraphDataSource

// TODO: check if it still runs and move to caps-examples
object CSVDemo {

  implicit lazy val sparkSession = SparkSession
    .builder()
    .config(new SparkConf(true))
    .master(MasterAddress.get)
    .appName(s"cypher-for-apache-spark-benchmark-${Calendar.getInstance().getTime}")
    .getOrCreate()

  def cypher(query: String): CypherResult = {
    println(s"Now executing query: $query")

    implicit val caps = CAPSSession.create()
    val dataSource = new FileCsvPropertyGraphDataSource(rootPath = "/demo")
    val graph = dataSource.graph(GraphName("ldbc_1"))
    val result = graph.cypher(query)

    val start = System.currentTimeMillis()
    println(s"Returned ${result.records.size} row(s) in ${System.currentTimeMillis() - start} ms")

    result
  }
}
