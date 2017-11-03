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

import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.caps.api.value.{CypherMap, CypherNode, CypherRelationship}

trait CypherValueEncoders extends LowPriorityCypherValueEncoders {
  implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
  implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] =
    kryo[CypherRelationship]
  implicit def cypherMapEncoder: ExpressionEncoder[CypherMap] = kryo[CypherMap]
}
