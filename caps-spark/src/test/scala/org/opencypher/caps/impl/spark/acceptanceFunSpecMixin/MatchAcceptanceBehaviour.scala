package org.opencypher.caps.impl.spark.acceptanceFunSpecMixin

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag

trait MatchAcceptanceBehaviour { this: AcceptanceTest =>

  def matchAcceptance(initGraph: String => CAPSGraph): Unit = {
    it("matches a trivial query") {
      // Given
      val given = initGraph(
        """
          |CREATE (p:Person {firstName: "Alice", lastName: "Foo"})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a:Person)
          |RETURN a.firstName
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(CypherMap("a.firstName" -> "Aice")))
    }
  }

}
