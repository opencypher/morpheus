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
package org.opencypher.caps.impl.spark.io.neo4j

import java.net.{URI, URLDecoder}

import org.apache.http.client.utils.URIBuilder
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.spark.io.neo4j.external.Neo4jConfig
import org.opencypher.caps.impl.spark.io.{CAPSGraphSourceFactoryCompanion, CAPSPropertyGraphDataSourceFactoryImpl}

case object Neo4JPropertyGraphDataSourceFactory extends CAPSGraphSourceFactoryCompanion("bolt", "bolt+routing")

case class Neo4JPropertyGraphDataSourceFactory() extends CAPSPropertyGraphDataSourceFactoryImpl(Neo4JPropertyGraphDataSourceFactory) {

  override protected def sourceForURIWithSupportedScheme(uri: URI)(
    implicit capsSession: CAPSSession): Neo4JPropertyGraphDataSourceOld = {
    val (user, passwd) = getUserInfo(uri)
    val boltUri = new URIBuilder()
      .setScheme(uri.getScheme)
      .setUserInfo(uri.getUserInfo)
      .setHost(uri.getHost)
      .setPort(uri.getPort)
      .build()

    val neo4jConfig = Neo4jConfig(boltUri, user, passwd, encrypted = false)
    Neo4JPropertyGraphDataSourceOld(neo4jConfig, getQueries(uri))
  }

  private def getUserInfo(uri: URI) = uri.getUserInfo match {
    case null => "" -> None

    case info =>
      val tokens = info.split(":")
      if (tokens.size != 2) throw IllegalArgumentException("values for username:password")
      tokens(0) -> Some(tokens(1))
  }

  private def getQueries(uri: URI) = uri.getQuery match {
    case null => None

    case queries =>
      val tokens = queries.split(";")
      val nodeQuery = tokens.headOption.getOrElse(throw IllegalArgumentException("a node query"))
      val relQuery = tokens.tail.headOption.getOrElse(throw IllegalArgumentException("a relationship query"))
      Some(URLDecoder.decode(nodeQuery, "UTF-8") -> URLDecoder.decode(relQuery, "UTF-8"))
  }
}
