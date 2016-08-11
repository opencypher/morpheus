package org.opencypher.spark.impl.frame

import org.opencypher.spark.CypherTypes.CTRelationship
import org.opencypher.spark.EntityData._
import org.opencypher.spark.{BinaryRepresentation, CypherValue}

class AllRelationshipsTest extends StdFrameTestSuite {

  import CypherValue.implicits._
  import factory._

  test("AllRelationships produces all input relationships") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))
    val n3 = add(newNode.withLabels("C").withProperties("name" -> "Xuxu"))
    val r1 = add(newUntypedRelationship(n1 -> n2))
    val r2 = add(newUntypedRelationship(n1 -> n3))
    val r3 = add(newUntypedRelationship(n2 -> n3))
    val rels = session.createDataset(List(r1, r2, r3))

    val result = AllRelationships(rels)('r).frameResult

    result.signature shouldHaveFields 'r -> CTRelationship
    result.signature shouldHaveFieldSlots 'r -> BinaryRepresentation
    result.toSet should equal(Set(r1, r2, r3))
  }
}
