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
package org.opencypher.okapi.impl.configuration

import scala.util.Try

abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
  def set(v: String): Unit = System.setProperty(name, v)

  def get: T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)

  override def toString: String = {
    val padded = name.padTo(25, " ").mkString("")
    s"$padded = $get"
  }
}

abstract class ConfigFlag(name: String, defaultValue: Boolean = false)
  extends ConfigOption[Boolean](name, defaultValue)(s => Try(s.toBoolean).toOption) {

  def set(): Unit = set(true.toString)

  def isSet: Boolean = get
}
