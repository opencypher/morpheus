package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import scala.collection.mutable.{Stack,ListBuffer}
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
  private val lineIndention = "\t\t"
  private val escapeStringMarks = "\"\"\""


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
          |import scala.util.{Failure, Success, Try}
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite with ScanGraphInit {""".stripMargin


    val testCases = "\n" + scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at compilation
      else
        generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path/$packageName/$className.scala")
    val fileString =
      s"""$classHeader
         |$testCases
         |}""".stripMargin
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }

  private def stringEscape(s: String): String = {
    s.replace("\n", "\\n").replace("\t", "\\t")
  }

  private def alignQuery(query: String): String = {
    query.replace("\n", s"\n$lineIndention\t")
  }


  private def stepsToString(steps: List[(Step, Int)]): String = {
    //todo: don't use immutable object
    val contextStack = Stack[(Execute, Int)]()
    val paremeters = ListBuffer.empty[String]

    //as parameter sometimes have invalid variable identifier
    def renameParameter(query: String) : String = {
      paremeters.fold(query)((acc, par) => acc.replace("$" + par,"$p" + par).replace("$`" + par + "`","$p" + par))
    }

    steps.map {
      case (Parameters(p, _), _) => p.foldLeft("") { (acc, x) =>
        paremeters += x._1
        acc +
          s"""
             |$lineIndention val p${x._1} = "${x._2}"
             |""".stripMargin
      }
      case (ex@Execute(_, querytype, _), nr) =>
        querytype match {
          case ExecQuery =>
            contextStack.push((ex, nr))
            ""
          case _ =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries not supported yet")
        }
      case (ExpectResult(expectedResult, _, sorted), _) =>
        val (contextQuery, stepNumber) = contextStack.pop()
        //result -> expected
        val resultRows = if (sorted)
          s"result${stepNumber}ValueRecords.rows" -> expectedResult.rows
        else
          s"result${stepNumber}ValueRecords.rows.sortBy(_.hashCode())" -> expectedResult.rows.sortBy(_.hashCode())

        s"""
           |val result${stepNumber} = graph.cypher(s$escapeStringMarks${alignQuery(renameParameter(contextQuery.query))}$escapeStringMarks)
           |
               |${lineIndention}val result${stepNumber}ValueRecords = CypherToTCKConverter.convertToTckStrings(result${stepNumber}.records).asValueRecords
           |${lineIndention}StringEscapeUtils.escapeJava(result${stepNumber}ValueRecords.header.toString()) should equal("${StringEscapeUtils.escapeJava(stringEscape(expectedResult.header.toString))}")
           |${lineIndention}StringEscapeUtils.escapeJava(${resultRows._1}.toString()) should equal("${StringEscapeUtils.escapeJava(stringEscape(resultRows._2.toString))}")
           """.stripMargin
      case (ExpectError(errorType, errorPhase, detail, _), _) =>
        val (contextQuery, stepNumber) = contextStack.pop()
        s"""
           |${lineIndention}val errorMessage$stepNumber  = an[Exception] shouldBe thrownBy{graph.cypher(s$escapeStringMarks${alignQuery(renameParameter(contextQuery.query))}$escapeStringMarks)}
           """.stripMargin
      case (SideEffects(expected, _), _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
          s"${lineIndention}fail() //TODO: side effects not handled yet"
        else
          ""
      case _ => ""
    }.filter(_.nonEmpty).mkString(s"\n ${lineIndention}")
  }


  private def generateTest(scenario: Scenario, black: Boolean): String = {
    val (initSteps, execSteps) = scenario.steps.partition {
      case Execute(_, InitQuery, _) => true
      case _ => false
    }

    val initQuery = escapeStringMarks + initSteps.foldLeft("")((combined, x) => x match {
      case Execute(query, InitQuery, _) => combined + s"\n$lineIndention\t" + alignQuery(query)
      case _ => combined
    }) + escapeStringMarks

    val xyz = stepsToString(scenario.steps.filter { case Execute(_, InitQuery, _) => false
    case _ => true
    }.zipWithIndex)


    val testString =
      s"""
         |    val graph = ${if (initSteps.nonEmpty) s"initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    $xyz
       """.stripMargin

    if (black)
      s"""  it("${scenario.name}") {
         |      Try({
         |        $testString
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
         |    $testString
         |  }
      """.stripMargin
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
