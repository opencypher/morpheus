package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.{symbols => neo4j}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTNumber}

class toCosTypeTest extends StdTestSuite {

  test("should convert basic types") {
    toCosType(neo4j.CTBoolean) shouldBe CTBoolean
    toCosType(neo4j.CTInteger) shouldBe CTInteger
    toCosType(neo4j.CTFloat) shouldBe CTFloat
    toCosType(neo4j.CTNumber) shouldBe CTNumber
  }
}
