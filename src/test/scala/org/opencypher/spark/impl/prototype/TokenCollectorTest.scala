package org.opencypher.spark.impl.prototype

import org.opencypher.spark.StdTestSuite

class TokenCollectorTest extends StdTestSuite {

  import CypherParser._

  test("collect tokens") {
    val given = parse("MATCH (a:Person)-[r:KNOWS]->(b:Duck) RETURN a.name, r.since, b.quack")
    val actual = TokenCollector(given)
    val expected = TokenDefs
      .none
      .withLabel(LabelDef("Duck"))
      .withLabel(LabelDef("Person"))
      .withRelType(RelTypeDef("KNOWS"))
      .withPropertyKey(PropertyKeyDef("name"))
      .withPropertyKey(PropertyKeyDef("since"))
      .withPropertyKey(PropertyKeyDef("quack"))

    actual should equal(expected)
  }
}




