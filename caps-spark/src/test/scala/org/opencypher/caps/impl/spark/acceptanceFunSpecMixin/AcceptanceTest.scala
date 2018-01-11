package org.opencypher.caps.impl.spark.acceptanceFunSpecMixin

import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.api.value.instances.AllInstances
import org.opencypher.caps.api.value.syntax.AllSyntax
import org.opencypher.caps.impl.record.CAPSRecordHeader._
import org.opencypher.caps.impl.spark.physical.RuntimeContext
import org.opencypher.caps.test.{CAPSTestSuite, TestSparkSession}
import org.opencypher.caps.test.support.DebugOutputSupport
import org.opencypher.caps.test.support.creation.caps.CAPSGraphFactory
import org.opencypher.caps.test.support.creation.propertygraph.CAPSPropertyGraphFactory
import org.scalatest.{Assertion, FunSpec, Matchers}

import scala.collection.Bag
import scala.collection.JavaConverters._
import scala.collection.immutable.HashedBagConfiguration

abstract class AcceptanceTest extends CAPSTestSuite {

  def capsGraphFactory: CAPSGraphFactory

  val initGraph: String => CAPSGraph = (createQuery) => capsGraphFactory(CAPSPropertyGraphFactory(createQuery))

  describe("using " + capsGraphFactory.name) {
    describe("and run match acceptance tests") {
      it should behave like it("matches a trivial query") {
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
        result.records.toMaps should equal(Bag(CypherMap("a.firstName" -> "Alice")))
      }
    }
  }
}


