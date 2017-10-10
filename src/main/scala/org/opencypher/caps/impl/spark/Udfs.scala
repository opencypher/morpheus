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
package org.opencypher.caps.impl.spark

import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.instances._
import org.opencypher.caps.api.value.syntax._
import org.opencypher.caps.impl.convert.toJavaType
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api.Label

import scala.collection.mutable

object Udfs {

  def const(v: CypherValue): () => Any = () => toJavaType(v)

  // TODO: Try to share code with cypherFilter()
  def lt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) < CypherValue(rhs)).orNull

  def lteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) <= CypherValue(rhs)).orNull

  def gteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) >= CypherValue(rhs)).orNull

  def gt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) > CypherValue(rhs)).orNull

  def getNodeLabels(labelNames: Seq[Label]): (Any) => Array[String] = {
    case a: mutable.WrappedArray[_] =>
      a.zip(labelNames).collect {
        case (true, label) => label.name
      }.toArray
  }

  def getNodeKeys(keyNames: Seq[String]): (Any) => Array[String] = {
    case a: mutable.WrappedArray[_] =>
      a.zip(keyNames).collect {
        case (v, key) if v != null => key
      }.toArray.sorted
    case x => Raise.invalidArgument("an array", x.toString)
  }

  def in[T](elem: Any, list: Any): Boolean = list match {
    case a: mutable.WrappedArray[_] => a.contains(elem)
    case x => Raise.invalidArgument("an array", x.toString)
  }

}
