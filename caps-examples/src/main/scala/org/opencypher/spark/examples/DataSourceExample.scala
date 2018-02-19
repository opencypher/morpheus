package org.opencypher.spark.examples

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.io.GraphName

object DataSourceExample extends App {

  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  session.mount(GraphName("sn"), socialNetwork)

  val result = session.cypher("FROM GRAPH AT 'session.sn' MATCH (n) RETURN n")

  result.print
}
