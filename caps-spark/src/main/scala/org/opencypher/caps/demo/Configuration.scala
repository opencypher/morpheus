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
package org.opencypher.caps.demo

import scala.util.Try

object Configuration {

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def set(v: String): Unit = System.setProperty(name, v)

    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)

    override def toString: String = {
      val filled = name + (name.length to 25).map(_ => " ").reduce(_ + _)
      s"$filled = ${get()}"
    }
  }

  object MasterAddress extends ConfigOption("caps.master", "local[*]")(Some(_))
  object Logging extends ConfigOption("caps.logging", "OFF")(Some(_))

  object PrintLogicalPlan extends ConfigOption("caps.explain", false)(s => Try(s.toBoolean).toOption) {
    def set(): Unit = set(true.toString)
  }

  object PrintPhysicalPlan extends ConfigOption("caps.explainPhysical", false)(s => Try(s.toBoolean).toOption) {
    def set(): Unit = set(true.toString)
  }

  object DebugPhysicalResult extends ConfigOption("caps.debugPhysical", false)(s => Try(s.toBoolean).toOption) {
    def set(): Unit = set(true.toString)
  }

  object PrintQueryExecutionStages extends ConfigOption("caps.stages", false)(s => Try(s.toBoolean).toOption) {
    def set(): Unit = set(true.toString)
  }

  object DefaultLabel extends ConfigOption("caps.defaultLabel", "")(Some(_))

  object DefaultType extends ConfigOption("caps.defaultType", "")(Some(_))

  val conf = Seq(MasterAddress, Logging)

  def print(): Unit = {
    conf.foreach(println)
  }

}
