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
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator

import scala.collection.mutable

object RuntimeContext {
  val empty = RuntimeContext(CypherMap.empty, _ => None, mutable.Map.empty)
}

case class RuntimeContext(
  parameters: CypherMap,
  resolve: URI => Option[PropertyGraph],
  cache: collection.mutable.Map[PhysicalOperator, PhysicalResult]
)

case object udfUtils {
  def initArray(): Any = {
    Array[Long]()
  }

  def arrayAppend(array: Any, next: Any): Any = {
    array match {
      case a: mutable.WrappedArray[_] =>
        a :+ next
    }
  }

  def contains(array: Any, elem: Any): Any = {
    array match {
      case a: mutable.WrappedArray[_] =>
        a.contains(elem)
    }
  }
}
