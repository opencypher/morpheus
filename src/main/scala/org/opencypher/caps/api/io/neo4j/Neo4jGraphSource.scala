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
package org.opencypher.caps.api.io.neo4j

import java.net.{URI, URLDecoder}

import org.neo4j.driver.v1.Config
import org.opencypher.caps.api.io.neo4j.Neo4jGraphSourceFactory.protocols
import org.opencypher.caps.api.io.{CreateOrFail, GraphSource, GraphSourceFactory, PersistMode}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class Neo4jGraphSource(config: EncryptedNeo4jConfig,
                            nodeQuery: String,
                            relQuery: String)
  extends GraphSource {

  override def sourceForGraphAt(uri: URI): Boolean =
    protocols.contains(uri.getScheme) && uri.getHost == config.uri.getHost && uri.getPort == config.uri.getPort

  override def graph(implicit capsSession: CAPSSession): CAPSGraph = {
    Neo4jGraphLoader.fromNeo4j(config, nodeQuery, relQuery)
  }

  /**
    * A canonical uri describing the location of this graph source.
    * The sourceForGraphAt function is guaranteed to return true for this uri.
    *
    * @return a uri describing the location of this graph source.
    */
  override def canonicalURI: URI = {
    val uri = config.uri
    val host = uri.getHost
    val port = if (uri.getPort == -1) "" else s":${uri.getPort}"
    val canonicalURIString = s"${uri.getScheme}://$host$port"
    URI.create(canonicalURIString)
  }

  /**
    * Create a new empty graph stored in this graph source.
    *
    * @param capsSession the session tied to the graph.
    * @return the graph stored in this graph source.
    * @throws RuntimeException if the graph could not be created or there already was a graph
    */
  override def create(implicit capsSession: CAPSSession): CAPSGraph =
    persist(CreateOrFail, CAPSGraph.empty)

  /**
    * Persists the argument graph to this source.
    *
    * @param mode        the persist mode to use.
    * @param graph       the graph to persist.
    * @param capsSession the session tied to the graph.
    * @return the persisted graph
    */
  override def persist(mode: PersistMode, graph: CAPSGraph)(implicit capsSession: CAPSSession): CAPSGraph =
    ???

  /**
    * Delete the graph stored at this graph source
    *
    * @param capsSession the session tied to the graph.
    */
  override def delete(implicit capsSession: CAPSSession): Unit =
    ???
}

object Neo4jGraphSourceFactory extends GraphSourceFactory {

  override val protocols = Set("bolt", "bolt+routing")

  override def sourceFor(uri: URI): Neo4jGraphSource = {
    val (user, passwd) = getUserInfo(uri)
    val (nodeQuery, relQuery) = getQueries(uri)

    Neo4jGraphSource(new EncryptedNeo4jConfig(uri, user, passwd, Config.EncryptionLevel.NONE), nodeQuery, relQuery)
  }

  private def getUserInfo(uri: URI) = uri.getUserInfo match {
    case null => "" -> None

    case info =>
      val tokens = info.split(":")
      if (tokens.size != 2) Raise.invalidArgument("username:password", "nothing")
      tokens(0) -> Some(tokens(1))
  }

  private def getQueries(uri: URI) = uri.getQuery match {
    case null => Raise.invalidArgument("node and relationship query", "none")

    case queries =>
      val tokens = queries.split(";")
      val nodeQuery = tokens.headOption.getOrElse(Raise.invalidArgument("a node query", "none"))
      val relQuery = tokens.tail.headOption.getOrElse(Raise.invalidArgument("a relationship query", "none"))
      URLDecoder.decode(nodeQuery, "UTF-8") -> URLDecoder.decode(relQuery, "UTF-8")
  }
}
