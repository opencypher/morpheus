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
package org.opencypher.spark_legacy.api.frame

import org.opencypher.spark.api.types.CypherType

trait CypherSlot {
  self: Serializable =>

  // Unique name of this slot; fixed at creation
  def sym: Symbol

  // The actual expression whose evaluation result is stored in this slot
  // def expr: Set[CypherExpression]

  // Corresponding data frame representation type

  def cypherType: CypherType

  def representation: Representation

  def ordinal: Int
}
