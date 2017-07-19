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
package org.opencypher.spark_legacy.impl

import org.apache.spark.sql._
import org.opencypher.spark_legacy.api.CypherResult
import org.opencypher.spark_legacy.impl.frame.{ProductAsMap, ProductAsRow}
import org.opencypher.spark.api.value.CypherValue

class StdRowResult(frame: StdCypherFrame[Row])(implicit val context: StdRuntimeContext) extends CypherResult[Row] {

  def signature = frame.signature

  override def toDS: Dataset[Row] = toDF

  override def toDF: DataFrame = {
    val out = frame.run
    out
  }
}

class StdProductResult(frame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Product] {

  def signature = frame.signature

  override def toDS: Dataset[Product] = {
    val out = frame.run
    out
  }

  override def toDF: DataFrame = {
    val out = ProductAsRow(frame).run
    out
  }
}


class StdRecordResult(productFrame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Map[String, CypherValue]] {

  def signature = frame.signature

  private lazy val frame: StdCypherFrame[Map[String, CypherValue]] = {
    ProductAsMap(productFrame)
  }

  override def toDS: Dataset[Map[String, CypherValue]] = {
    val out = frame.run
    out
  }

  override def toDF: DataFrame = {
    val out = ProductAsRow(productFrame).run
    out
  }
}
