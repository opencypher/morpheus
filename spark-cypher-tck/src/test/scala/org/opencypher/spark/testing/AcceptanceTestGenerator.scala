package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.tools.tck.api._
import org.scalatest.prop.TableFor1
import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.tools.tck.constants.TCKErrorPhases


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)
  private val lineIndention = "\t\t"


  private def generateClassFiles(featureName: String, scenarios: TableFor1[Scenario], black: Boolean) = {
    val path = s"spark-cypher-testing/src/test/scala/org/opencypher/spark/impl/acceptance/"
    val packageName = if (black) "blackList" else "whiteList"
    val className = s"${featureName}_$packageName"
    val classHeader =
      s"""|package org.opencypher.spark.impl.acceptance.$packageName
          |
          |import org.scalatest.junit.JUnitRunner
          |import org.junit.runner.RunWith
          |import org.opencypher.okapi.tck.test.CypherToTCKConverter
          |import org.opencypher.spark.testing.CAPSTestSuite
          |import org.opencypher.spark.impl.acceptance.ScanGraphInit
          |import org.apache.commons.lang.StringEscapeUtils
          |import org.opencypher.spark.impl.graph.CAPSGraphFactory
          |${if (black) "import scala.util.{Failure, Success, Try}" else ""}
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite with ScanGraphInit {""".stripMargin


    val testCases = "\n" + scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at compilation
      else
        generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path/$packageName/$className.scala")
    val fileString = classHeader + testCases + "}"
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }
  //result consists of (step-type, step-string)
  private def stepToStringTuple(step: Step): (String, String) = {
    step match {
      case Execute(query, querytype, _) =>
        val alignedQuery = query.replace("\n", s"\n$lineIndention\t")
        querytype match {
          case InitQuery => "init" -> alignedQuery
          case ExecQuery => "exec" -> alignedQuery //todo: handle control query like in "Should store duration" (possible via foldLeft? (needs changes of context steps))
          case SideEffectQuery =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries not supported yet")
        }
      case ExpectResult(expectedResult: CypherValueRecords, _, sorted) =>
        //escapeJava in Order to escape result-strings
        if (sorted)
          "result_sorted" -> s"""StringEscapeUtils.escapeJava(CypherToTCKConverter.convertToTckStrings(result.records).asValueRecords.toString) should equal("${StringEscapeUtils.escapeJava(expectedResult.toString())}") """
        else
          "result_unsorted" ->
            //equal "equalsUnordered" Method of CypherValueRecords (own version here as only strings can be compared)
            s"""
               |${lineIndention}val resultValueRecords = CypherToTCKConverter.convertToTckStrings(result.records).asValueRecords
               |${lineIndention}StringEscapeUtils.escapeJava(resultValueRecords.header.toString()) should equal("${StringEscapeUtils.escapeJava(expectedResult.header.toString())}")
               |${lineIndention}StringEscapeUtils.escapeJava(resultValueRecords.rows.sortBy(_.hashCode()).toString()) should equal("${StringEscapeUtils.escapeJava(expectedResult.rows.sortBy(_.hashCode()).toString())}")""".stripMargin

      case ExpectError(errorType, TCKErrorPhases.RUNTIME, detail, _) =>
        "error" -> errorType //todo: also return detail
      case SideEffects(expected, _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
          "sideeffect" -> s"//TODO: handle side effects"
        /*Todo: calculate via before and after State? (can result graph return nodes/relationships/properties/labels as a set of cyphervalues?)
        todo: maybe also possible via Cypher-Queries (may take too long?) */
        else
          "" -> ""
      case _ => "" -> ""
    }
  }

  private def generateTest(scenario: Scenario, black: Boolean): String = {

    val escapeStringMarks = "\"\"\""
    val steps = scenario.steps.map {
      stepToStringTuple
    }.filter(_._2.nonEmpty)

    val initSteps = steps.filter(_._1.equals("init"))
    val initQuery = escapeStringMarks + initSteps.foldLeft("")((combined, x) => combined + s"\n$lineIndention\t" + x._2) + escapeStringMarks


    val executionQuerySteps = steps.filter(_._1.equals("exec")).map(_._2) //handle control query?
    val expectedResultSteps = steps.filter(_._1.startsWith("result")).map(_._2)
    val expectedErrorSteps = steps.filter(_._1.equals("error")).map(_._2) //only one error can be expected
    val sideEffectSteps = steps.filter(_._1.eq("sideeffect")).map(_._2)

    //todo allow f.i. expected error only for 2nd executionQuery
    val tests = executionQuerySteps.zipWithIndex.zipAll(expectedResultSteps, "", "").zipAll(expectedErrorSteps, "", "").zipAll(sideEffectSteps, "", "").map { case ((((v, w), x), y), z) => (v, w, x, y, z) }

    val testStrings = tests.map {
      case (exec: String, num: Integer, result: String, expectedError: String, sideEffect: String) =>
        //todo: !! how to check for expectedErrorType?
        s"""
           |    val result$num = graph.cypher($escapeStringMarks$exec$escapeStringMarks)
           |    ${result.replace("result", s"result$num")}
           |    ${if (expectedError.nonEmpty) "fail() //todo: check expected Error" else ""}
           |    ${if (sideEffect.nonEmpty) "fail() //todo: check side effects" else ""}
        """.stripMargin
    }


    val result = if (black)
      s"""  it("${scenario.name}") {
         |      Try({
         |        val graph = ${if(initSteps.nonEmpty) s"initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |        ${testStrings.mkString("\n        ")}
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
         |    val graph = ${if(initSteps.nonEmpty) s"initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    ${testStrings.mkString("\n    ")}
         |  }
      """.stripMargin

    result
  }

  //todo: clear directories before first write?
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
