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
package org.opencypher.caps.impl.spark.io.neo4j.external

import org.apache.spark.Partition

private class Neo4jPartition(idx: Long = 0, skip: Long = 0, limit: Long = Long.MaxValue) extends Partition {
  override def index: Int = idx.toInt

  val window: Map[String, Any] = Map("_limit" -> limit, "_skip" -> skip)

  override def toString: String = s"Neo4jRDD index $index skip $skip limit: $limit"
}
