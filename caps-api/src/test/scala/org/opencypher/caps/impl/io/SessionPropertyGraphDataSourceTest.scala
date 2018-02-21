package org.opencypher.caps.impl.io

import org.opencypher.caps.api.io.GraphName
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

class SessionPropertyGraphDataSourceTest extends FunSuite with MockitoSugar with Matchers {

  test("hasGraph should return true for existing graph") {
    val source = new SessionPropertyGraphDataSource
    val testGraphName = GraphName.from("test")
    source.store(testGraphName, null)
    source.hasGraph(testGraphName) should be(true)
  }

  test("hasGraph should return false for non-existing graph") {
    val source = new SessionPropertyGraphDataSource
    val testGraphName = GraphName.from("test")
    source.hasGraph(testGraphName) should be(false)
  }

  test("graphNames should return all names of stored graphs") {
    val source = new SessionPropertyGraphDataSource
    val testGraphName1 = GraphName.from("test1")
    val testGraphName2 = GraphName.from("test2")

    source.graphNames should equal(Set.empty)

    source.store(testGraphName1, null)
    source.store(testGraphName2, null)
    source.graphNames should equal(Set(testGraphName1, testGraphName2))
  }

}
