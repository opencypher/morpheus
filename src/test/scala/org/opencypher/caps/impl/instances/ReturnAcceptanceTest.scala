package org.opencypher.caps.impl.instances

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.value.CypherMap

import scala.collection.Bag

class ReturnAcceptanceTest extends CAPSTestSuite {

  test("return node") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.records.toMaps should equal(Bag(
      CypherMap("n" -> 0),
      CypherMap("n" -> 1))
    )
  }

  test("return node with details") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("n" -> 0, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> "bar"),
      CypherMap("n" -> 1, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> null))
    )
  }

  test("return rel") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    result.records.toMaps should equal(Bag(
      CypherMap("r" -> 0),
      CypherMap("r" -> 1)
    ))
  }

  test("return rel with details") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    val relId = result.recordsWithDetails.space.tokens.registry.relTypeRefByName(DEFAULT_LABEL).id

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("r" -> 0, "source(r)" -> 0, "target(r)" -> 1, "type(r)" -> relId, "r.foo" -> "bar"),
      CypherMap("r" -> 1, "source(r)" -> 1, "target(r)" -> 2, "type(r)" -> relId, "r.foo" -> null)
    ))
  }
}
