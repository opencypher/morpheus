package org.opencypher.caps.api.io.neo4j

import java.net.{URI, URLDecoder}

import org.neo4j.driver.v1.Config
import org.opencypher.caps.api.io.GraphSourceFactoryCompanion
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.io.GraphSourceFactoryImpl

case object Neo4jGraphSourceFactory extends GraphSourceFactoryCompanion("bolt", "bolt+routing")

case class Neo4jGraphSourceFactory()
  extends GraphSourceFactoryImpl[Neo4jGraphSource](Neo4jGraphSourceFactory) {

  override protected def sourceForVerifiedURI(uri: URI): Neo4jGraphSource = {
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
