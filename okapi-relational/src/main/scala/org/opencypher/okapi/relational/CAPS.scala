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
package org.opencypher.okapi.relational

import java.util.Properties

import scala.io.Source
import scala.language.postfixOps
import scala.util.Try

// TODO: Remove?
object CAPS {

  self =>

  def version: Option[String] = {
    val clazz = self.getClass
    Try {
      // 1) Try to read from maven descriptor
      val inputStream = clazz.getResourceAsStream("/META-INF/maven/org.opencypher/cypher-for-apache-spark/pom.properties")
      val properties = new Properties()
      properties.load(inputStream)
      value(properties.getProperty("version"))
    } orElse Try {
      // 2) Use clazz package implementation version
      value(clazz.getPackage.getImplementationVersion)
    } orElse Try {
      // 3) Use clazz package specification version as a fallback
      value(clazz.getPackage.getSpecificationVersion)
    } orElse Try {
      // 4) We're likely running in some development setup; attempt to read from guessed location of version.txt
      value[String](Source.fromFile("target/classes/version.txt", "UTF-8").mkString)
    } orElse Try {
      // 5) We're likely running in some development setup; attempt to read from other guessed location of version.txt
      value[String](Source.fromFile("main/resources/version.txt", "UTF-8").mkString)
    } orElse Try {
      // 6) Nothing worked? Hopefully someone set us a property to report
      value(System.getProperty("project.version"))
    } toOption
  }

  private def value[T](v: T) = Option(v).getOrElse(throw new NullPointerException)
}
