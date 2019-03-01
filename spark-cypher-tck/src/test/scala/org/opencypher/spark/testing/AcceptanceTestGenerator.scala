package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.tools.tck.api.Scenario


object AcceptanceTestGenerator {
  private val path = s"../spark-cypher-testing/src/test/scala/org/opencypher/spark/impl/acceptance/"
  private val packageHeader = "package org.opencypher.spark.impl.acceptance"
  private val imports =
    """|import org.scalatest.junit.JUnitRunner
       |import org.junit.runner.RunWith
       |import org.opencypher.spark.testing.CAPSTestSuite
       |import org.opencypher.spark.impl.acceptance.ScanGraphInit
       |
                        """.stripMargin

  //todo: fix annotation formatted wrong
  private val classHeader =
    """|@RunWith(classOf[JUnitRunner])
       |class AggregationTests extends CAPSTestSuite with ScanGraphInit {""".stripMargin

  def generateTestFiles(scenarios: ScenariosFor): Unit = {
    val black = scenarios.blackList.groupBy(_.featureName)
    val white = scenarios.whiteList.groupBy(_.featureName)

    white.map(feature => {
      val packageName = "whiteList"
      val WhitePackageHeader = s"$packageHeader.$packageName \n \n"
      val file = new File(s"$path/$packageName/${feature._1}.scala")

      //todo: evaluate scenarios
      val testcases = "\n" + feature._2.map(scenario => generateScenarioToTest(scenario, white = true)).mkString("\n")



      val classString = WhitePackageHeader + imports + classHeader + testcases + "}" //todo: add testcases to string
      val out = new PrintWriter(file)
      out.print(classString)
      out.close()
      file.createNewFile()
      feature._1
    })

  }

  //todo
  private def generateScenarioToTest(scenario: Scenario, white: Boolean): String = {
    s"//test for ${scenario.name} \n"
  }

}
