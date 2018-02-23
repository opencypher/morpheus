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
package org.opencypher.caps.cosc.impl.value

import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.exception.IllegalArgumentException

object CypherTypeOps {

  implicit class OrderingCypherType(ct: CypherType) {
    def ordering: Ordering[_] = ct match {
      case CTBoolean => Ordering[Boolean]
      case CTFloat => Ordering[Float]
      case CTInteger => Ordering[Long]
      case CTString => Ordering[String]
      case _ => throw IllegalArgumentException("Cypher type with ordering support", ct)
    }

    def equivalence: Equiv[_] = ct match {
      case CTBoolean => Equiv[Boolean]
      case CTFloat => Equiv[Float]
      case CTInteger => Equiv[Long]
      case CTString => Equiv[String]
      case _ => throw IllegalArgumentException("Cypher type with equivalence support", ct)
    }
  }

}
