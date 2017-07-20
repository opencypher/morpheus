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

object Configuration {

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)

    override def toString: String = {
      val filled = name + (name.length to 25).map(_ => " ").reduce(_ + _)
      s"$filled = ${get()}"
    }
  }

  object GraphSize extends ConfigOption("cos.graph-size", -1l)(x => Some(java.lang.Long.parseLong(x)))
  object MasterAddress extends ConfigOption("cos.master", "local[*]")(Some(_))
  object Logging extends ConfigOption("cos.logging", "OFF")(Some(_))
  object Partitions extends ConfigOption("cos.shuffle-partitions", 40)(x => Some(java.lang.Integer.parseInt(x)))
  object Runs extends ConfigOption("cos.runs", 6)(x => Some(java.lang.Integer.parseInt(x)))
  object WarmUpRuns extends ConfigOption("cos.warmupRuns", 2)(x => Some(java.lang.Integer.parseInt(x)))
  object NodeFilePath extends ConfigOption("cos.nodeFile", "<>")(Some(_))
  object RelFilePath extends ConfigOption("cos.relFile", "<>")(Some(_))
  object Neo4jAddress extends ConfigOption("cos.neo4j-address", "bolt://ff01adf3.databases.neo4j.io")(Some(_))
  object Neo4jUser extends ConfigOption("cos.neo4j-user", "openCypher_tests")(Some(_))
  object Neo4jPassword extends ConfigOption("cos.neo4j-pw", "try-planet-stand-art")(Some(_))
  object Benchmarks extends ConfigOption("cos.benchmarks", "frames")(Some(_))
  object Query extends ConfigOption("cos.query", 5)(x => Some(java.lang.Integer.parseInt(x)))

  val conf = Seq(GraphSize, MasterAddress, Logging, Partitions, Runs, WarmUpRuns, NodeFilePath,
    RelFilePath, Neo4jPassword, Benchmarks, Query)

  def print(): Unit = {
    conf.foreach(println)
  }

}
