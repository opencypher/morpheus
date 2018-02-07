package org.opencypher.caps.cosc

import org.opencypher.caps.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.caps.cosc.value.{COSCNode, COSCRelationship}

object Demo extends App {

  implicit val coscSession: COSCSession = COSCSession.create

  val graph = COSCGraph.create(DemoData.nodes, DemoData.rels)

  graph.cypher("MATCH (n) WHERE n.age > 40 RETURN n").print
}

object DemoData {

  def nodes = Seq(alice, bob)

  def rels = Seq(aliceKnowsBob)

  val aliceId = 0L
  val alice = COSCNode(
    aliceId,
    Set("Person"),
    CypherMap(
      "name" -> CypherString("Alice"),
      "age" -> CypherInteger(42)
    )
  )

  val bobId = 1L
  val bob = COSCNode(
    bobId,
    Set("Person"),
    CypherMap(
      "name" -> CypherString("Bob"),
      "age" -> CypherInteger(23)
    )
  )

  val aliceKnowsBob = COSCRelationship(
    0L,
    aliceId,
    bobId,
    "KNOWS",
    CypherMap("since" -> CypherInteger(2018))
  )
}
