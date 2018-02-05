package org.opencypher.caps.cosc

import org.opencypher.caps.api.value._

object Demo extends App {

  implicit val coscSession: COSCSession = COSCSession.create

  val graph = COSCGraph(DemoData.nodes, DemoData.rels)

  graph.cypher("MATCH (n) RETURN n").print
}

object DemoData {

  def nodes = Seq(alice, bob)

  def rels = Seq(aliceKnowsBob)

  val aliceId = EntityId(0L)
  val alice = CAPSNode(
    aliceId,
    Seq("Person"),
    Properties(
      "name" -> CAPSValue("Alice"),
      "age" -> CAPSValue(42)
    )
  )

  val bobId = EntityId(1L)
  val bob = CAPSNode(
    bobId,
    Seq("Person"),
    Properties(
      "name" -> CAPSValue("Bob"),
      "age" -> CAPSValue(23)
    )
  )

  val aliceKnowsBob = CAPSRelationship(
    EntityId(0L),
    aliceId,
    bobId,
    "KNOWS",
    Properties(
      "since" -> CAPSValue(2018)
    )
  )
}
