package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.{symbols => neo4j}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTNumber}

class toCosTypesTest extends StdTestSuite {

  test("should convert basic types") {
    toCosTypes(neo4j.CTBoolean) shouldBe CTBoolean
    toCosTypes(neo4j.CTInteger) shouldBe CTInteger
    toCosTypes(neo4j.CTFloat) shouldBe CTFloat
    toCosTypes(neo4j.CTNumber) shouldBe CTNumber
  }
}
