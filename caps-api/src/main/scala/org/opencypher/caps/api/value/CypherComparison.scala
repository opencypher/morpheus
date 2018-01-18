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
package org.opencypher.caps.api.value

import org.opencypher.caps.api.types._

sealed trait CypherComparison extends Any {
  def map(f: Int => Boolean): Option[Ternary]
}

sealed trait Incomparable extends CypherComparison

final case class SuccessfulComparison(cmp: Int) extends AnyVal with CypherComparison {
  override def map(f: Int => Boolean): Option[Ternary] = Some(Ternary(f(cmp)))
}

case object IncompatibleArguments extends Incomparable {
  override def map(f: Int => Boolean): Option[Ternary] = None
}

case object ArgumentComparesNulls extends Incomparable {
  override def map(f: Int => Boolean): Option[Ternary] = Some(Maybe)
}
