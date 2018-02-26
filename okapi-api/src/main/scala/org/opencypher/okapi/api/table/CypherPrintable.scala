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
package org.opencypher.okapi.api.table

import java.io.PrintStream

import org.opencypher.okapi.impl.util.PrintOptions

object CypherPrintable {

  def apply(s: String): CypherPrintable = new CypherPrintable {
    override def show(implicit options: PrintOptions): Unit =
      options.stream.print(s)
  }

}

// TODO: Inline into CypherRecords directly
trait CypherPrintable {

  final def printTo(stream: PrintStream)(implicit options: PrintOptions): Unit =
    show(options.stream(stream))

  def show(implicit options: PrintOptions): Unit
}
