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
package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.spark.SparkColumnName

import scala.annotation.tailrec
import scala.util.Random

object FreshVariableNamer {
  val NAME_SIZE = 5
  val PREFIX = "  "

  @tailrec
  def generateUniqueName(header: RecordHeader): String = {
    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = SparkColumnName.from(Some(String.valueOf(chars.toArray)))

    if (header.slots.map(SparkColumnName.of).contains(name)) generateUniqueName(header)
    else name
  }

  def apply(seed: String, t: CypherType): Var = Var(s"$PREFIX$seed")(t)
}
