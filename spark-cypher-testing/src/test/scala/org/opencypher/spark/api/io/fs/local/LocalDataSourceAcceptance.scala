package org.opencypher.spark.api.io.fs.local

import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory

abstract class LocalDataSourceAcceptance extends CAPSTestSuite with CAPSPGDSAcceptance {

  protected def createDs(graph: CAPSGraph): CAPSPropertyGraphDataSource

  protected var tempDir = new TemporaryFolder()

  override def initSession(): CAPSSession = caps

  override protected def beforeEach(): Unit = {
    tempDir.create()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    tempDir.delete()
    tempDir = new TemporaryFolder()
    super.afterEach()
  }

  override def create(graphName: GraphName, testGraph: InMemoryTestGraph, createStatements: String): PropertyGraphDataSource = {
    val graph = CAPSScanGraphFactory(testGraph)
    val ds = createDs(graph)
    ds.store(graphName, graph)
    ds
  }

}
