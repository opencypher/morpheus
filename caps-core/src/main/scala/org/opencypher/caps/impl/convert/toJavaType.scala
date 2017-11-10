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
package org.opencypher.caps.impl.convert

import org.opencypher.caps.api.value._

object toJavaType extends Serializable {

  // TODO: Tests
  // Inverse operation found in CypherValue.apply()
  def apply(v: CypherValue): Any = v match {
    case CypherInteger(long) => long
    case CypherString(s)     => s
    case CypherBoolean(b)    => b
    case CypherFloat(f)      => f
    case CypherList(list)    => list.map(apply)
    case x =>
      throw new IllegalArgumentException(s"Expected a (representation of a) Cypher value, but was $x (${x.getClass})")
  }
}
