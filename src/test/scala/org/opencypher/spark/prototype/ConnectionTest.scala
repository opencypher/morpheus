package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir.Field
import org.opencypher.spark.prototype.ir.pattern.{DirectedRelationship, UndirectedRelationship}

class ConnectionTest extends StdTestSuite {

  val field_a = Field("a")
  val field_b = Field("b")
  val field_c = Field("c")

  test("SimpleConnection.flip") {
    DirectedRelationship(field_a, field_b).flip should equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a).flip should equal(DirectedRelationship(field_a, field_a))
  }

  test("SimpleConnection.equals") {
    DirectedRelationship(field_a, field_b) shouldNot equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a) should equal(DirectedRelationship(field_a, field_a))
    DirectedRelationship(field_a, field_a) shouldNot equal(DirectedRelationship(field_a, field_b))
  }

  test("UndirectedConnection.flip") {
    UndirectedRelationship(field_a, field_b).flip should equal(UndirectedRelationship(field_b, field_a))
  }

  test("UndirectedConnection.equals") {
    UndirectedRelationship(field_a, field_b) should equal(UndirectedRelationship(field_b, field_a))
    UndirectedRelationship(field_c, field_c) should equal(UndirectedRelationship(field_c, field_c))
  }

  test("Mixed equals") {
    DirectedRelationship(field_a, field_a) should equal(UndirectedRelationship(field_a, field_a))
  }
}
