package org.opencypher.caps.api.io.neo4j

import java.net.URI

import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}

case class Neo4JGraphSource(uri: URI, username: String, password: String, queries: String*) extends GraphSource {

  override def handles(uri: URI): Boolean = ???

  override def get(implicit capsSession: CAPSSession): CAPSGraph = ???
}

object Neo4jGraphSourceFactory extends GraphSourceFactory {

  override val protocol = "bolt"

  override def fromURI(uri: URI): Neo4JGraphSource = ???
}
