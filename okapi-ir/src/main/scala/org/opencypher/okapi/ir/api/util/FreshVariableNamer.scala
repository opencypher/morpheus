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
package org.opencypher.okapi.ir.api.util

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.ir.api.expr.Var

object FreshVariableNamer {
  val PREFIX = "  "

  def apply(seed: String, t: CypherType): Var = Var(s"$PREFIX$seed")(t)

  def apply(id: Int, t: CypherType): Var = apply(s"FRESH_VAR$id", t)
}
