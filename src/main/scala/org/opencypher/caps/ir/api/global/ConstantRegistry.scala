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
package org.opencypher.caps.ir.api.global

import org.opencypher.caps.common.RefCollection
import org.opencypher.caps.impl.syntax.register._

import scala.util.Try

object ConstantRegistry {
  val empty = ConstantRegistry(RefCollection.empty)
}

// Constants are globals that are provided as part of invoking a query
final case class ConstantRegistry(constants: RefCollection[Constant] = RefCollection.empty[Constant]) {

  self =>

  def constantByName(name: String): Constant = constant(constantRefByName(name))
  def constant(ref: ConstantRef): Constant = constants.lookup(ref).get

  def constantRefByName(name: String): ConstantRef = constants.findByKey(name).get
  def constantRef(defn: Constant): ConstantRef = constantRefByName(defn.name)

  def withConstant(defn: Constant): ConstantRegistry = {
    constants.insert(defn) match {
      case Right((Some(newConstants), _)) => copy(constants = newConstants)
      case _ => self
    }
  }

  def contains(name: String): Boolean = Try(constantRefByName(name)).isSuccess
}
