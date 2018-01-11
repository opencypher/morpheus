package org.opencypher.caps.impl.spark.acceptanceFunSpecMixin
import org.opencypher.caps.test.support.creation.caps.{CAPSGraphFactory, CAPSScanGraphFactory}

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSGraphFactory = CAPSScanGraphFactory
}
