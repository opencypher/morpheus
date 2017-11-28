/*
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
package org.opencypher.caps.api.spark.encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.caps.api.value.CypherValue

import scala.language.implicitConversions

trait LowPriorityCypherValueEncoders {
  implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
  implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
  implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
}

