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
package org.opencypher.caps.impl.spark

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.value.{CypherValue, NullableCypherValue}
import org.opencypher.caps.ir.api.Label

import scala.collection.mutable

object Udfs {

  def const(v: Any): () => Any = () => v

  def getNodeLabels(labelNames: Seq[Label]): (Any) => Array[String] = {
    case a: mutable.WrappedArray[_] =>
      a.zip(labelNames)
        .collect {
          case (true, label) => label.name
        }
        .toArray
  }

  def getNodeKeys(keyNames: Seq[String]): (Any) => Array[String] = {
    case a: mutable.WrappedArray[_] =>
      a.zip(keyNames)
        .collect {
          case (v, key) if v != null => key
        }
        .toArray
        .sorted
    case x => throw IllegalArgumentException("an array", x)
  }

  def in[T](elem: Any, list: Any): Boolean = list match {
    case a: mutable.WrappedArray[_] => a.contains(elem)
    case x => throw IllegalArgumentException("an array", x)
  }

}
