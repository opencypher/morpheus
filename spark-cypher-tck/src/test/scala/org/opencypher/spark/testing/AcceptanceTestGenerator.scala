package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.okapi.api.value.CypherValue.{CypherList => OKAPICypherList, CypherMap => OKAPICypherMap, CypherNull => OKAPICypherNull, CypherString => OKAPICypherString, CypherValue => OKAPICypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.okapi.tck.test.TckToCypherConverter.tckValueToCypherValue
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.{Connection, Backward => TCKBackward, CypherOrderedList => TCKCypherOrderedList, CypherUnorderedList => TCKUnorderedList,CypherBoolean => TCKCypherBoolean, CypherFloat => TCKCypherFloat, CypherInteger => TCKCypherInteger, CypherList => TCKCypherList, CypherNode => TCKCypherNode, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath, CypherProperty => TCKCypherProperty, CypherPropertyMap => TCKCypherPropertyMap, CypherRelationship => TCKCypherRelationship, CypherString => TCKCypherString, CypherValue => TCKCypherValue, Forward => TCKForward}
import org.scalatest.prop.TableFor1

import scala.collection.mutable


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)
  private val lineIndention = "\t\t"
  private val escapeStringMarks = "\"\"\""

  case class ResultRows(queryResult: String, expected: List[Map[String, TCKCypherValue]])

  private def generateClassFiles(featureName: String, scenarios: TableFor1[Scenario], black: Boolean) = {
    val path = s"spark-cypher-testing/src/test/scala/org/opencypher/spark/impl/acceptance/" //or here?spark-cypher-tck/src/test/scala/org/opencypher/spark/testing/
    val packageName = if (black) "blackList" else "whiteList"
    val className = featureName
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
          |import org.opencypher.okapi.api.value.CypherValue._
          |import scala.util.{Failure, Success, Try}
          |import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, CypherString => TCKCypherString, CypherOrderedList => TCKCypherOrderedList,
          |   CypherNode => TCKCypherNode, CypherRelationship => TCKCypherRelationship, Connection, Forward => TCKForward, Backward => TCKBackward,
          |   CypherInteger => TCKCypherInteger, CypherFloat => TCKCypherFloat, CypherBoolean => TCKCypherBoolean, CypherProperty => TCKCypherProperty,
          |   CypherPropertyMap => TCKCypherPropertyMap, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath}
          |
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite with ScanGraphInit {""".stripMargin


    val testCases = "\n" + scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at scala-compilation
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

  private def alignQuery(query: String): String = {
    query.replaceAllLiterally("\n", s"\n$lineIndention\t")
  }

  private def escapeString(s: String): String = {
    s""" "${StringEscapeUtils.escapeJava(s)}" """
  }

  //todo: move to better place
  private def tckConnectionToCreateString(con: Connection): String = {
    con match {
      case TCKForward(r, n) => s"TCKForward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
      case TCKBackward(r, n) => s"TCKBackward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
    }
  }
  //todo: move to better place
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
      case l : TCKCypherList =>
        l match {
          case TCKCypherOrderedList(elems) => s"TCKCypherOrderedList(List(${elems.map(tckCypherValueToCreateString).mkString(",")}))"
          case _ => s"TCKCypherValue.apply(${escapeString(l.toString)}, false)"
        }
      case TCKCypherNull => "TCKCypherNull"
      case TCKCypherNode(labels, properties) => s"TCKCypherNode(Set(${labels.map(escapeString).mkString(",")}), ${tckCypherValueToCreateString(properties)})"
      case TCKCypherRelationship(typ, properties) => s"TCKCypherRelationship(${escapeString(typ)}, ${tckCypherValueToCreateString(properties)})"
      case TCKCypherPath(start, connections) => s"TCKCypherPath(${tckCypherValueToCreateString(start)},List(${connections.map(tckConnectionToCreateString).mkString(",")}))"
      case other =>
        throw NotImplementedException(s"Converting Cypher value $value of type `${other.getClass.getSimpleName}`")
    }
  }

  //todo refactor into cypherToCreateString object ?
  private def cypherValueToCreateString(value: OKAPICypherValue): String = {
    value match {
      case OKAPICypherList(l) => "List(" + l.map(cypherValueToCreateString).mkString(",") + ")"
      case OKAPICypherMap(m) => "CypherMap(" + m.map { case (key, cv) => s"(${escapeString(key)},${cypherValueToCreateString(cv)})" }.mkString(",") + ")"
      case OKAPICypherString(s) => escapeString(s)
      case OKAPICypherNull => "CypherNull"
      case _ => s"${value.getClass.getSimpleName}(${value.unwrap})"
    }
  }

  private def tckCypherMapToTCKCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapString = cypherMap.map { case (key, cypherValue) => s""" "$key" """ -> tckCypherValueToCreateString(cypherValue) }
    s"Map(${mapString.mkString(",")})"
  }

  private def tckCypherMapToOkapiCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapString = cypherMap.map { case (key, tckCypherValue) => s""" "$key" """ -> cypherValueToCreateString(tckValueToCypherValue(tckCypherValue)) }
    s"CypherMap(${mapString.mkString(",")})"
  }

  private def stepsToString(steps: List[(Step, Int)]): String = {
    //todo: don't use immutable object
    val contextStack = mutable.Stack[(Execute, Int)]()
    val contextParameters = mutable.Stack[Int]()

    steps.map {
      case (Parameters(p, _), stepnr) => contextParameters.push(stepnr)
        s"val parameter$stepnr = ${tckCypherMapToOkapiCreateString(p)}"
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
        val parameters = if (contextParameters.nonEmpty) "," + s"parameter${contextParameters.pop()}" else ""

        val (contextQuery, stepNumber) = contextStack.pop()
        //result -> expected
        val resultRows = if (sorted)
          ResultRows(s"result${stepNumber}ValueRecords.rows", expectedResult.rows)
        else
          ResultRows(s"result${stepNumber}ValueRecords.rows.sortBy(_.hashCode())", expectedResult.rows.sortBy(_.hashCode()))
        //todo: format expectedResultCreation String
        s"""
           |${lineIndention}val result$stepNumber = graph.cypher($escapeStringMarks${alignQuery(contextQuery.query)}$escapeStringMarks$parameters)
           |
           |${lineIndention}val result${stepNumber}ValueRecords = CypherToTCKConverter.convertToTckStrings(result$stepNumber.records).asValueRecords
           |${lineIndention}result${stepNumber}ValueRecords.header should equal(List(${expectedResult.header.map(escapeString).mkString(",")}))
           |$lineIndention${resultRows.queryResult} should equal(List(${resultRows.expected.map(tckCypherMapToTCKCreateString).mkString(s", \n$lineIndention$lineIndention$lineIndention")}))
           """.stripMargin
      case (ExpectError(errorType, errorPhase, detail, _), _) =>
        val (contextQuery, stepNumber) = contextStack.pop()
        //todo: check for errorType and detail (when coresponding errors exist like SyntaxError, TypeError, ParameterMissing, ...)
        s"""
           |${lineIndention}val errorMessage$stepNumber  = an[Exception] shouldBe thrownBy{graph.cypher($escapeStringMarks${alignQuery(contextQuery.query)}$escapeStringMarks)}
           """.stripMargin

      case (SideEffects(expected, _), _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
          s"${lineIndention}fail() //TODO: side effects not handled yet"
        else
          ""
      case _ => ""
    }.filter(_.nonEmpty).mkString(s"\n $lineIndention")
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

    //todo: get parameterContext beforehand and execContext beforehand --> use of immutable stacks possible
    val execString = stepsToString(execSteps.zipWithIndex)

    val testString =
      s"""
         |    val graph = ${if (initSteps.nonEmpty) s"initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    $execString
       """.stripMargin

    if (black)
      s"""  it("${scenario.name}") {
         |    Try({
         |        $testString
         |    }) match{
         |      case Success(_) =>
         |        throw new RuntimeException(s"A blacklisted scenario actually worked")
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
