package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.tools.tck.api._
import org.scalatest.prop.TableFor1
import org.apache.commons.lang.StringEscapeUtils


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)

  private def generateClassFiles(featureName: String, scenarios: TableFor1[Scenario], black: Boolean) = {
    val path = s"spark-cypher-testing/src/test/scala/org/opencypher/spark/impl/acceptance/"
    val packageName = if (black) "blackList" else "whiteList"
    val className = s"${featureName}_$packageName"
    val classHeader =
      s"""|package org.opencypher.spark.impl.acceptance.$packageName
          | import org.scalatest.junit.JUnitRunner
          |import org.junit.runner.RunWith
          |import org.opencypher.okapi.tck.test.CypherToTCKConverter
          |import org.opencypher.spark.testing.CAPSTestSuite
          |import org.opencypher.spark.impl.acceptance.ScanGraphInit
          |${if (black) "import scala.util.{Failure, Success, Try}" else ""}
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite with ScanGraphInit {""".stripMargin


    val testCases = "\n" + scenarios.map(scenario =>
      if(scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at compilation
      else
      generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path/$packageName/$className.scala")
    val fileString = classHeader + testCases + "}"
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }


  private def generateTest(scenario: Scenario, black: Boolean): String = {
    val steps = scenario.steps.map {
      case Execute(query, querytype, _) =>
        val alignedQuery = query.replace("\n", "\n\t\t\t\t\t")
        val escapedQuery = "\"\"\"" + alignedQuery+ "\"\"\""
        querytype match {
          case InitQuery => "init" -> alignedQuery
          case ExecQuery => "exec" -> s"val result = graph.cypher($escapedQuery)" //todo: handle control query like in "Should store duration" (possible via foldLeft? (needs changes of context steps))
          case SideEffectQuery =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries not supported yet")
        }
      case ExpectResult(expectedResult, _, sorted) =>
        //todo: maybe just compare strings?
        if (sorted)
          "result_sorted" -> s"""CypherToTCKConverter.convertToTckStrings(result.records) should equal("${StringEscapeUtils.escapeJava(expectedResult.rows.toString())}") """
        else
        //todo: test for unordered things using Bag? (difficult with check via string?)
          "result_unsorted" -> s"""result.records.toMapsWithCollectedEntities should equal("${StringEscapeUtils.escapeJava(expectedResult.rows.toSet.toString())}")"""

      case ExpectError(errorType, phase, detail, _) =>
        "error" -> errorType //todo: think about better string generator as error string needs to encapsulate execQuery String
      case SideEffects(expected, _) =>
        //check if relevant Side-Effects exist
        if (expected.v.exists(_._2 > 0))
          "sideeffect" -> s"//TODO: handle side effects"
        /*Todo: calculate via before and after State? (can result graph return nodes/relationships/properties/labels as a set of cyphervalues?)
          todo: maybe also possible via Cypher-Queries (may take too long?) */
        else
          "" -> ""
      case _ => "" -> ""
    }.filter(_._2.nonEmpty)


    val initQuery = "\"\"\"" + steps.filter(_._1.equals("init")).foldLeft("")((combined,x) => combined + "\n\t\t\t\t\t" + x._2) + "\"\"\""

    val executionQueries = steps.filter(_._1.equals("exec")) //handle control query?
    val expectedResultSteps = steps.filter(_._1.startsWith("result"))
    val expectedError = steps.filter(_._1.equals("error")) //only one error can be expected
    val sideEffects = steps.filter(_._1.eq("sideeffect"))

    //todo start from here (and build strings via map from here)
    val excQueries = executionQueries.zipAll(expectedResultSteps,"","").zipAll(expectedError,"","").zipAll(sideEffects,"","").map{ case (((a,x),y), z) => (a, x, y, z) }

    //todo refactor in extra stepString
    val modsteps = steps.filterNot(_._1.equals("init")).map(_._2)


    val result = if (black)
      s"""  it("${scenario.name}") {
         |      Try({
         |        val graph = initGraph($initQuery)
         |        ${modsteps.mkString("\n        ")}
         |      }) match{
         |        case Success(_) =>
         |          throw new RuntimeException(s"A blacklisted scenario actually worked")
         |        case Failure(_) =>
         |          ()
         |      }
         |    }
      """.stripMargin
    else
      s"""  it("${scenario.name}") {
         |    val graph = initGraph("$initQuery")
         |    ${modsteps.mkString("\n    ")}
         |  }
      """.stripMargin

    result
  }

  val blackFeatures = scenarios.blackList.groupBy(_.featureName)
  val whiteFeatures = scenarios.whiteList.groupBy(_.featureName)

  whiteFeatures.map { feature => {
    generateClassFiles(feature._1, feature._2, black = false)
  }
  }

  blackFeatures.map { feature => {
    generateClassFiles(feature._1, feature._2, black = true)
  }
  }

}
