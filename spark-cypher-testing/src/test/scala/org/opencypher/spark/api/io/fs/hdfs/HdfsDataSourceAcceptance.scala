package org.opencypher.spark.api.io.fs.hdfs

import org.apache.hadoop.fs.Path
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.testing.fixture.MiniDFSClusterFixture
import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory

import scala.collection.JavaConverters._


abstract class HdfsDataSourceAcceptance  extends CAPSTestSuite with CAPSPGDSAcceptance with MiniDFSClusterFixture {

  protected def createDs(graph: CAPSGraph): CAPSPropertyGraphDataSource

  override def initSession(): CAPSSession = caps

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    val fs = cluster.getFileSystem()
    fs.listStatus(new Path("/")).foreach { f =>
      fs.delete(f.getPath, true)
    }
    super.afterEach()
  }

  override def create(graphName: GraphName, testGraph: InMemoryTestGraph, createStatements: String): PropertyGraphDataSource = {
    val graph = CAPSScanGraphFactory(testGraph)

    // DO NOT DELETE THIS! If the config is not cleared, the PGDSAcceptanceTest fails because of connecting to a
    // non-existent HDFS cluster. We could not figure out why the afterEach call in MiniDFSClusterFixture does not handle
    // the clearance of the config correctly.
    resetHadoopConfig()

    val ds = createDs(graph)
    ds.store(graphName, graph)
    ds
  }

  def resetHadoopConfig(): Unit = {
    session.sparkContext.hadoopConfiguration.clear()
    val hadoopParams = clusterConfig.asScala
    for (entry <- hadoopParams) {
      session.sparkContext.hadoopConfiguration.set(entry.getKey, entry.getValue)
    }
  }
}
