package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.okapi.api.value.CypherValue.{CypherList => OKAPICypherList, CypherMap => OKAPICypherMap, CypherNull => OKAPICypherNull, CypherString => OKAPICypherString, CypherValue => OKAPICypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.okapi.tck.test.TckToCypherConverter.tckValueToCypherValue
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.{Backward => TCKBackward, CypherBoolean => TCKCypherBoolean, CypherFloat => TCKCypherFloat, CypherInteger => TCKCypherInteger, CypherList => TCKCypherList, CypherNode => TCKCypherNode, CypherNull => TCKCypherNull, CypherOrderedList => TCKCypherOrderedList, CypherPath => TCKCypherPath, CypherProperty => TCKCypherProperty, CypherPropertyMap => TCKCypherPropertyMap, CypherRelationship => TCKCypherRelationship, CypherString => TCKCypherString, CypherUnorderedList => TCKUnorderedList, CypherValue => TCKCypherValue, Forward => TCKForward}
import org.scalatest.prop.TableFor1

import scala.collection.mutable


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)
  private val escapeStringMarks = "\"\"\""
  private val path = s"spark-cypher-tck/src/test/scala/org/opencypher/spark/testing/"
  private val packageNames = Map("white" -> "whiteList", "black" -> "blackList")

  case class ResultRows(queryResult: String, expected: List[Map[String, TCKCypherValue]])

  private def generateClassFile(featureName: String, scenarios: TableFor1[Scenario], black: Boolean) = {
    val packageName = if (black) packageNames.get("black") else packageNames.get("white")
    val className = featureName
    val classHeader =
      s"""|package org.opencypher.spark.testing.${packageName.get}
          |
          |import org.scalatest.junit.JUnitRunner
          |import org.junit.runner.RunWith
          |import org.opencypher.okapi.tck.test.CypherToTCKConverter
          |import org.opencypher.spark.testing.CAPSTestSuite
          |import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory
          |import org.opencypher.spark.impl.graph.CAPSGraphFactory
          |import org.opencypher.okapi.api.value.CypherValue._
          |import scala.util.{Failure, Success, Try}
          |import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, CypherString => TCKCypherString, CypherOrderedList => TCKCypherOrderedList,
          |   CypherNode => TCKCypherNode, CypherRelationship => TCKCypherRelationship, Connection, Forward => TCKForward, Backward => TCKBackward,
          |   CypherInteger => TCKCypherInteger, CypherFloat => TCKCypherFloat, CypherBoolean => TCKCypherBoolean, CypherProperty => TCKCypherProperty,
          |   CypherPropertyMap => TCKCypherPropertyMap, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath}
          |
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite{""".stripMargin


    val testCases = scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at scala-compilation
      else
        generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path/${packageName.get}/$className.scala")

    val fileString =
      s"""$classHeader
         |
         |$testCases
         |}""".stripMargin
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }

  private def alignQuery(query: String): String = {
    query.replaceAllLiterally("\n", s"\n      ")
  }

  private def escapeString(s: String): String = {
    s""" "${StringEscapeUtils.escapeJava(s)}" """
  }

  private def tckCypherValueToCreateString(value: TCKCypherValue): String = {
    value match {
      case TCKCypherString(v) => s"TCKCypherString(${escapeString(v)})"
      case TCKCypherInteger(v) => s"TCKCypherInteger(${v}L)"
      case TCKCypherFloat(v) => s"TCKCypherFloat($v)"
      case TCKCypherBoolean(v) => s"TCKCypherBoolean($v)"
      case TCKCypherProperty(key, v) => s"""TCKCypherProperty("${escapeString(key)}",${tckCypherValueToCreateString(v)})"""
      case TCKCypherPropertyMap(properties) =>
        val propertiesCreateString = properties.map { case (key, v) => s"(${escapeString(key)}, ${tckCypherValueToCreateString(v)})" }.mkString(",")
        s"TCKCypherPropertyMap(Map($propertiesCreateString))"
      case l: TCKCypherList =>
        l match {
          case TCKCypherOrderedList(elems) => s"TCKCypherOrderedList(List(${elems.map(tckCypherValueToCreateString).mkString(",")}))"
          case _ => s"TCKCypherValue.apply(${escapeString(l.toString)}, false)"
        }
      case TCKCypherNull => "TCKCypherNull"
      case TCKCypherNode(labels, properties) => s"TCKCypherNode(Set(${labels.map(escapeString).mkString(",")}), ${tckCypherValueToCreateString(properties)})"
      case TCKCypherRelationship(typ, properties) => s"TCKCypherRelationship(${escapeString(typ)}, ${tckCypherValueToCreateString(properties)})"
      case TCKCypherPath(start, connections) =>
        val connectionsCreateString = connections.map {
          case TCKForward(r, n) => s"TCKForward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
          case TCKBackward(r, n) => s"TCKBackward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
        }.mkString(",")

        s"TCKCypherPath(${tckCypherValueToCreateString(start)},List($connectionsCreateString))"
      case other =>
        throw NotImplementedException(s"Converting Cypher value $value of type `${other.getClass.getSimpleName}`")
    }
  }

  private def cypherValueToCreateString(value: OKAPICypherValue): String = {
    value match {
      case OKAPICypherList(l) => s"List(${l.map(cypherValueToCreateString).mkString(",")})"
      case OKAPICypherMap(m) =>
        val mapElementsString = m.map {
          case (key, cv) => s"(${escapeString(key)},${cypherValueToCreateString(cv)})"
        }.mkString(",")
        s"CypherMap($mapElementsString)"
      case OKAPICypherString(s) => escapeString(s)
      case OKAPICypherNull => "CypherNull"
      case _ => s"${value.getClass.getSimpleName}(${value.unwrap})"
    }
  }

  private def tckCypherMapToTCKCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, cypherValue) => escapeString(key) -> tckCypherValueToCreateString(cypherValue)
    }
    s"Map(${mapElementsString.mkString(",")})"
  }

  private def tckCypherMapToOkapiCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, tckCypherValue) => escapeString(key) -> cypherValueToCreateString(tckValueToCypherValue(tckCypherValue))
    }
    s"CypherMap(${mapElementsString.mkString(",")})"
  }

  private def stepsToString(steps: List[(Step, Int)]): String = {
    val contextStack = mutable.Stack[(Execute, Int)]()
    val contextParameterStepNrs = mutable.Stack[Int]()

    steps.map {
      case (Parameters(p, _), stepNr) => contextParameterStepNrs.push(stepNr)
        s"val parameter$stepNr = ${tckCypherMapToOkapiCreateString(p)}"
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
        val parameters = if (contextParameterStepNrs.nonEmpty) s", parameter${contextParameterStepNrs.head}" else ""
        val (contextQuery, stepNumber) = contextStack.head
        val resultRows = if (sorted)
          ResultRows(s"result${stepNumber}ValueRecords.rows", expectedResult.rows)
        else
          ResultRows(s"result${stepNumber}ValueRecords.rows.sortBy(_.hashCode())", expectedResult.rows.sortBy(_.hashCode()))

        s"""
           |		val result$stepNumber = graph.cypher($escapeStringMarks${alignQuery(contextQuery.query)}$escapeStringMarks$parameters)
           |
           |		val result${stepNumber}ValueRecords = CypherToTCKConverter.convertToTckStrings(result$stepNumber.records).asValueRecords
           |		result${stepNumber}ValueRecords.header should equal(List(${expectedResult.header.map(escapeString).mkString(",")}))
           |		${resultRows.queryResult} should equal(List(${resultRows.expected.map(tckCypherMapToTCKCreateString).mkString(s", \n						")}))
           """.stripMargin
      case (ExpectError(errorType, errorPhase, detail, _), _) =>
        val (contextQuery, stepNumber) = contextStack.pop()
        //todo: check for errorType and detail (when coresponding errors exist like SyntaxError, TypeError, ParameterMissing, ...)
        s"""
           |		val errorMessage$stepNumber  = an[Exception] shouldBe thrownBy{graph.cypher($escapeStringMarks${alignQuery(contextQuery.query)}$escapeStringMarks)}
           """.stripMargin

      case (SideEffects(expected, _), _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
          //SideEffects not relevant in CAPS
          s"		fail() //TODO: side effects not handled yet"
        else
          ""
      case _ => ""
    }.filter(_.nonEmpty).mkString(s"\n 		")
  }


  private def generateTest(scenario: Scenario, black: Boolean): String = {
    val (initSteps, execSteps) = scenario.steps.partition {
      case Execute(_, InitQuery, _) => true
      case _ => false
    }

    val initQuery = escapeStringMarks + initSteps.foldLeft("")((combined, x) => x match {
      case Execute(query, InitQuery, _) => combined + s"\n				  ${alignQuery(query)}"
      case _ => combined
    }) + escapeStringMarks

    val execString = stepsToString(execSteps.zipWithIndex)
    val testString =
      s"""
         |    val graph = ${if (initSteps.nonEmpty) s"CAPSScanGraphFactory.initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    $execString
       """.stripMargin

    if (black)
      s"""  it("${scenario.name}") {
         |    Try({
         |     ${testString.replaceAll("\n    ","\n     ")}
         |    }) match{
         |      case Success(_) =>
         |        throw new RuntimeException(s"A blacklisted scenario may work (expected error cases can be false-positives)")
         |      case Failure(_) =>
         |    }
         |   }
      """.stripMargin
    else
      s"""  it("${scenario.name}") {
         |    $testString
         |  }
      """.stripMargin
  }


  val blackFeatures = scenarios.blackList.groupBy(_.featureName)
  val whiteFeatures = scenarios.whiteList.groupBy(_.featureName)

  //checks if package directories exists clears them or creates new
  packageNames.values.map(packageName => {
    val directory = new File(path + packageName)
    if (directory.exists()) {
      val files = directory.listFiles()
      files.map(_.delete())
    }
    else {
      directory.mkdir()
    }
  }
  )

  whiteFeatures.map { feature => {
    generateClassFile(feature._1, feature._2, black = false)
  }
  }

  blackFeatures.map { feature => {
    generateClassFile(feature._1, feature._2, black = true)
  }
  }

}
