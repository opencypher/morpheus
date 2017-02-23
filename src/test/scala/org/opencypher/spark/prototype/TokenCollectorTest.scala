package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir.token.{Label, PropertyKey, RelType, TokenRegistry}

class TokenCollectorTest extends StdTestSuite {

  import CypherParser._

  test("collect tokens") {
    val given = parse("MATCH (a:Person)-[r:KNOWS]->(b:Duck) RETURN a.name, r.since, b.quack")
    val actual = TokenCollector(given)
    val expected = TokenRegistry
      .none
      .withLabel(Label("Duck"))
      .withLabel(Label("Person"))
      .withRelType(RelType("KNOWS"))
      .withPropertyKey(PropertyKey("name"))
      .withPropertyKey(PropertyKey("since"))
      .withPropertyKey(PropertyKey("quack"))

    actual should equal(expected)
  }
}




