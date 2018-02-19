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
package org.opencypher.caps.impl.spark.io

import java.net.URI

import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.api.io.PropertyGraphDataSourceOld
import org.opencypher.caps.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.caps.impl.spark.io.session.SessionPropertyGraphDataSourceFactory

object CAPSGraphSourceHandler {

  def apply(allFactories: Set[CAPSPropertyGraphDataSourceFactory]): CAPSGraphSourceHandler = {
    val (sessionGraphSourceFactories, additionalGraphSourceFactories) = allFactories.partition {
      case _: SessionPropertyGraphDataSourceFactory => true
      case _ => false
    }
    val sessionGraphSourceFactory: SessionPropertyGraphDataSourceFactory = sessionGraphSourceFactories.headOption match {
      case Some(f: SessionPropertyGraphDataSourceFactory) => f
      case _ => throw IllegalStateException("There needs to be at least one session graph source factory")
    }

    CAPSGraphSourceHandler(sessionGraphSourceFactory, additionalGraphSourceFactories)
  }

}

case class CAPSGraphSourceHandler(
  sessionGraphSourceFactory: SessionPropertyGraphDataSourceFactory,
  additionalGraphSourceFactories: Set[CAPSPropertyGraphDataSourceFactory]) {
  private val factoriesByScheme: Map[String, CAPSPropertyGraphDataSourceFactory] = {
    val allFactories = additionalGraphSourceFactories + sessionGraphSourceFactory
    val entries = allFactories.flatMap(factory => factory.schemes.map(scheme => scheme -> factory))
    if (entries.size == entries.map(_._1).size)
      entries.toMap
    else
      throw IllegalArgumentException(
        "at most one graph source factory per URI scheme",
        s"factories for schemes: ${allFactories.map(factory => factory.name -> factory.schemes.mkString("[", ", ", "]")).mkString(",")}"
      )
  }

  def mountSourceAt(source: CAPSPropertyGraphDataSourceOld, uri: URI)(implicit capsSession: CypherSession): Unit =
    sessionGraphSourceFactory.mountSourceAt(source, uri)

  def unmountAll(implicit capsSession: CypherSession): Unit =
    sessionGraphSourceFactory.unmountAll(capsSession)

  def sourceAt(uri: URI)(implicit capsSession: CypherSession): PropertyGraphDataSourceOld =
    optSourceAt(uri).getOrElse(throw IllegalArgumentException(s"graph source for URI: $uri"))

  def optSourceAt(uri: URI)(implicit capsSession: CypherSession): Option[PropertyGraphDataSourceOld] =
    factoriesByScheme
      .get(uri.getScheme)
      .map(_.sourceFor(uri))
}
