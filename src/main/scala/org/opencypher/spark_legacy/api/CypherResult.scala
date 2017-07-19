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
package org.opencypher.spark_legacy.api

import org.apache.spark.sql.{DataFrame, Dataset}
import org.opencypher.spark_legacy.api.frame.CypherFrameSignature

trait CypherResult[T] {

  def signature: CypherFrameSignature

  def toDF: DataFrame
  def toDS: Dataset[T]

  def collectAsScalaList = toDS.collect().toList
  def collectAsScalaSet = toDS.collect().toSet

  def exhaust() = {
    val itr = toDS.toLocalIterator()
    while (itr.hasNext) itr.next()
  }

  def count(): Long = toDS.count()
}


