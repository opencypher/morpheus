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
package org.opencypher.spark_legacy

import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.api.value.CypherValue

import scala.collection.immutable.ListMap

package object api {

  object implicits extends CypherImplicits

  type TypedSymbol = (Symbol, CypherType)
  type Alias = (Symbol, Symbol)

  object CypherRecord {
    def apply(elts: (String, CypherValue)*): CypherRecord =
      ListMap(elts: _*)
  }

  // Keys guaranteed to be in column order
  type CypherRecord = Map[String, CypherValue]

}
