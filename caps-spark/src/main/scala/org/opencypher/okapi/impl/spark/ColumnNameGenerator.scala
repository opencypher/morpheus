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
package org.opencypher.okapi.impl.spark

import org.opencypher.okapi.relational.impl.table.{ColumnName, RecordHeader}

import scala.annotation.tailrec
import scala.util.Random

object ColumnNameGenerator {
  val NAME_SIZE = 5

  @tailrec
  def generateUniqueName(header: RecordHeader): String = {
    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = ColumnName.from(String.valueOf(chars.toArray))

    if (header.slots.map(ColumnName.of).contains(name)) generateUniqueName(header)
    else name
  }
}
