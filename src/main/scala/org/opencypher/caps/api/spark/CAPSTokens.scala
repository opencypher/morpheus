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
package org.opencypher.caps.api.spark

import org.opencypher.caps.ir.api.global._
import org.opencypher.caps.impl.record.CAPSRecordsTokens

// Lightweight wrapper around token registry to expose a simple lookup api for all tokens that may occur in a data frame
trait CAPSTokens {

  self: Serializable =>

  type Tokens <: CAPSTokens

  def labels: Set[String]
  def relTypes: Set[String]

  def labelName(id: Int): String
  def labelId(name: String): Int

  def relTypeName(id: Int): String
  def relTypeId(name: String): Int

  def withLabel(name: String): Tokens
  def withRelType(name: String): Tokens
}


object CAPSTokens {
  val empty = CAPSRecordsTokens(TokenRegistry.empty)
}
