package org.opencypher.okapi.impl.graph

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

class CypherCatalogTest extends FunSpec with MockitoSugar with Matchers  {
  it("avoids retrieving a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy new CypherCatalog().source(Namespace("foo"))
  }

  it("avoids retrieving a graph not stored in the session") {
    an[NoSuchElementException] should be thrownBy new CypherCatalog().graph(QualifiedGraphName("foo"))
  }

  it("avoids retrieving a graph from a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy new CypherCatalog().graph(QualifiedGraphName(Namespace("foo"), GraphName("bar")))
  }

  it("returns all available namespaces") {
    val catalog = new CypherCatalog()
    catalog.namespaces should equal(Set(SessionGraphDataSource.Namespace))
    val namespace = Namespace("foo")
    val dataSource = mock[PropertyGraphDataSource]
    catalog.registerSource(namespace, dataSource)
    catalog.namespaces should equal(Set(SessionGraphDataSource.Namespace, namespace))
  }
}
