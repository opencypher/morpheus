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
package org.opencypher.caps.api.record

import java.io.PrintStream

object CypherRecords {
  val DEFAULT_COLUMN_WIDTH = 20
}

trait CypherRecords {

  type Data
  type Records <: CypherRecords

  def header: RecordHeader
  def data: Data

  def fields: Set[String]
  def fieldsInOrder: Seq[String]

  def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): Records

  def compact: Records

  def print(columnWidth: Int = CypherRecords.DEFAULT_COLUMN_WIDTH): Unit
  def printTo(stream: PrintStream, columnWidth: Int = CypherRecords.DEFAULT_COLUMN_WIDTH): Unit
}
