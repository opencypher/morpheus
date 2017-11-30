package org.opencypher.caps

import java.io.File
import java.util

import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.caps.TCKAdapterForCAPS.AsTckGraph
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.test.support.TestGraph
import org.opencypher.tools.tck.api.{CypherTCK, Graph}

import scala.collection.JavaConverters._

class CAPSTest {
  implicit val caps: CAPSSession = CAPSSession.local()

  @TestFactory
  def runTCKOnTestGraph(): util.Collection[DynamicTest] = {
    def createTestGraph: Graph = TestGraph().graph

    val dynamicTests = CypherTCK.allTckScenarios.map { scenario =>
      val name = scenario.toString
      val executable = scenario(createTestGraph)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

  @TestFactory
  def runCustomOnTestGraph(): util.Collection[DynamicTest] = {
    def createTestGraph: Graph = TestGraph().graph

    val file = new File(getClass.getResource("CAPSTestFeature.feature").toURI)
    val dynamicTests = CypherTCK.parseFilesystemFeature(file).scenarios.map { scenario =>
      val name = scenario.toString
      val executable = scenario(createTestGraph)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

}
