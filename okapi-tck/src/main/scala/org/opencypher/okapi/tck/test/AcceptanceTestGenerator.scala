/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.tck.test

import java.io.{File, PrintWriter}

import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.CreateStringGenerator._
import org.opencypher.tools.tck.api._

//SideEffects not relevant in CAPS
case class AcceptanceTestGenerator(
  specificImports: List[String],
  graphFactoryName: String,
  createGraphMethodName: String,
  emptyGraphMethodName: String,
  testSuiteName: String,
  targetPackageName: String,
  checkSideEffects: Boolean,
  addGitIgnore: Boolean
) {
  private val escapeStringMarks = "\"\"\""
  private val packageNames = Map("white" -> "whiteList", "black" -> "blackList")

  //generates test-cases for given scenario names
  def generateGivenScenarios(outDir: File, resFiles: Array[File], keyWords: Array[String] = Array.empty): Unit = {
    setUpDirectories(outDir)
    val scenarios = getScenarios(resFiles)
    val wantedWhiteScenarios = scenarios.whiteList.filter(scen =>
      keyWords.map(keyWord =>
        scen.name
          .contains(keyWord))
        .reduce(_ || _))
    val wantedBlackScenarios = scenarios.blackList.filter(scen =>
      keyWords.map(keyWord =>
        scen.name
          .contains(keyWord))
        .reduce(_ || _))
    generateClassFile("specialWhiteCases", wantedWhiteScenarios, black = false, outDir)
    generateClassFile("specialBlackCases", wantedBlackScenarios, black = true, outDir)
  }

  def generateAllScenarios(outDir: File, resFiles: Array[File]): Unit = {
    setUpDirectories(outDir)
    val scenarios = getScenarios(resFiles)
    val blackFeatures = scenarios.blackList.groupBy(_.featureName)
    val whiteFeatures = scenarios.whiteList.groupBy(_.featureName)
    whiteFeatures.map { case (featureName, featureScenarios) =>
      generateClassFile(featureName, featureScenarios, black = false, outDir)
    }

    blackFeatures.map { case (featureName, featureScenarios) =>
      generateClassFile(featureName, featureScenarios, black = true, outDir)
    }
  }

  private def generateClassFile(className: String, scenarios: Seq[Scenario], black: Boolean, outDir: File) = {
    val packageName = if (black) packageNames.get("black") else packageNames.get("white")

    val testCases = scenarios
      .filterNot(_.name.equals("Failing on incorrect unicode literal")) //produced test couldn't be compiled
      .map(scenario => generateTest(scenario, black))
      .mkString("\n")

    val fileString =
      s"""/*
         | * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
         | *
         | * Licensed under the Apache License, Version 2.0 (the "License");
         | * you may not use this file except in compliance with the License.
         | * You may obtain a copy of the License at
         | *
         | *     http://www.apache.org/licenses/LICENSE-2.0
         | *
         | * Unless required by applicable law or agreed to in writing, software
         | * distributed under the License is distributed on an "AS IS" BASIS,
         | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
         | * See the License for the specific language governing permissions and
         | * limitations under the License.
         | *
         | * Attribution Notice under the terms of the Apache License 2.0
         | *
         | * This work was created by the collective efforts of the openCypher community.
         | * Without limiting the terms of Section 6, any Derivative Work that is not
         | * approved by the public consensus process of the openCypher Implementers Group
         | * should not be described as “Cypher” (and Cypher® is a registered trademark of
         | * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
         | * proposals for change that have been documented or implemented should only be
         | * described as "implementation extensions to Cypher" or as "proposed changes to
         | * Cypher that are not yet approved by the openCypher community".
         | */
         |package $targetPackageName.${packageName.get}
         |
         |import org.junit.runner.RunWith
         |import org.scalatestplus.junit.JUnitRunner
         |import scala.util.{Failure, Success, Try}
         |import org.opencypher.okapi.api.value.CypherValue._
         |import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
         |import org.opencypher.okapi.tck.test.CypherToTCKConverter._
         |import org.opencypher.okapi.tck.test.TCKGraph
         |import org.opencypher.tools.tck.SideEffectOps.Diff
         |import org.opencypher.tools.tck.api.CypherValueRecords
         |import org.opencypher.tools.tck.SideEffectOps
         |import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, CypherString => TCKCypherString, CypherOrderedList => TCKCypherOrderedList,
         |           CypherNode => TCKCypherNode, CypherRelationship => TCKCypherRelationship, Connection, Forward => TCKForward, Backward => TCKBackward,
         |           CypherInteger => TCKCypherInteger, CypherFloat => TCKCypherFloat, CypherBoolean => TCKCypherBoolean, CypherProperty => TCKCypherProperty,
         |           CypherPropertyMap => TCKCypherPropertyMap, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath}
         |${specificImports.mkString("\n")}
         |
         |@RunWith(classOf[JUnitRunner])
         |class $className extends $testSuiteName{
         |
         |$testCases
         |}""".stripMargin

    val file = new File(s"$outDir/${packageName.get}/$className.scala")
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }

  //checks if package directories exists clears them or creates new
  private def setUpDirectories(outDir: File): Unit = {
    if (outDir.exists() || outDir.mkdir())
      packageNames.values.map(packageName => {
        val directory = new File(outDir + "/" + packageName)
        if (directory.exists()) {
          val files = directory.listFiles()
          files.map(_.delete())
        }
        else {
          directory.mkdir()
        }
        val gitIgnoreFile = new File(outDir + "/" + packageName + "/.gitignore")
        val writer = new PrintWriter(gitIgnoreFile)
        writer.println("*")
        writer.close()
        gitIgnoreFile.createNewFile()
      }
      )
  }

  private def getScenarios(resFiles: Array[File]): ScenariosFor = {
    //todo: only take .txt or files with no suffix?
    val resFileNames = resFiles.map(_.getPath).filterNot(name => name.contains(".feature") || name.contains(".scala"))
    ScenariosFor(resFileNames: _*)
  }

  private def generateTest(scenario: Scenario, black: Boolean): String = {
    val (initSteps, execSteps) = scenario.steps.partition {
      case Execute(_, InitQuery, _) => true
      case _ => false
    }

    //combine multiple initQueries into one
    val initQueries = initSteps.collect {
      case Execute(query, InitQuery, _) => s"${alignString(query)}"
    }

    val initQueryString =
      if (initSteps.nonEmpty)
        s"""$graphFactoryName.$createGraphMethodName(
           |      $escapeStringMarks
           |        ${initQueries.mkString("\n")}
           |      $escapeStringMarks
           |    )""".stripMargin
      else graphFactoryName + '.' + emptyGraphMethodName

    val execString = stepsToString(execSteps.zipWithIndex)
    val testString =
      s"""
         |    val graph = $initQueryString
         |    $execString
       """.stripMargin

    val expectsError = scenario.steps.exists {
      case _: ExpectError => true
      case _ => false
    }

    if (black)
      s"""  it("${scenario.name}") {
         |    Try({
         |     ${alignString(testString, 1)}
         |    }) match{
         |      case Success(_) =>
         |        throw new RuntimeException("${if (expectsError) "False-positive as probably wrong error" else "A blacklisted scenario works"}")
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

  private def stepsToString(steps: List[(Step, Int)]): String = {
    case class stepContext(ExecQueryStepNr: Option[Int], ParameterStepNr: Option[Int], graphStateStepNr: Option[Int])

    steps.foldLeft((stepContext(None, None, None), "")) {
      case ((context, accString), (Parameters(p, _), stepNr)) =>
        val stepString = s"val parameter$stepNr = ${tckCypherMapToOkapiCreateString(p)}"
        stepContext(context.ExecQueryStepNr, Some(stepNr), context.graphStateStepNr) -> (accString + s"\n    $stepString")
      case ((context, accString), (_: Measure, stepNr)) =>
        if (checkSideEffects) {
          val stepString = s"val beforeState$stepNr = SideEffectOps.measureState(TCKGraph($graphFactoryName,graph))"
          stepContext(context.ExecQueryStepNr, context.ParameterStepNr, Some(stepNr)) -> (accString + s"\n    $stepString")
        }
        else context -> accString
      case ((context, accString), (Execute(query, queryType, _), stepNr)) =>
        queryType match {
          case ExecQuery =>
            val parameters = context.ParameterStepNr match {
              case Some(pStepNr) => s", parameter$pStepNr"
              case None => ""
            }
            val stepString =
              s"""
                 |    lazy val result$stepNr = graph.cypher(
                 |      $escapeStringMarks
                 |        ${alignString(query)}
                 |      $escapeStringMarks$parameters
                 |    )
           """
            stepContext(Some(stepNr), context.ParameterStepNr, context.graphStateStepNr) -> (accString + s"\n    $stepString")
          case _ =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries specified inside the scenario are not supported yet")
        }
      case ((context, accString), (ExpectResult(expectedResult: CypherValueRecords, _, sorted), _)) =>
        val resultNumber = context.ExecQueryStepNr match {
          case Some(stepNr) => stepNr
          case None => throw new IllegalStateException(s"no query found to compare result $expectedResult")
        }

        val equalMethod = if (sorted) "equals" else "equalsUnordered"
        val stepString =
          s"""
             |    val result${resultNumber}ValueRecords = convertToTckStrings(result$resultNumber.records).asValueRecords
             |    val expected${resultNumber}ValueRecords = CypherValueRecords(List(${expectedResult.header.map(escapeString).mkString(",")}),
             |      List(${expectedResult.rows.map(tckCypherMapToTCKCreateString).mkString(s", \n           ")}))
             |
           |    result${resultNumber}ValueRecords.$equalMethod(expected${resultNumber}ValueRecords) shouldBe true
           """.stripMargin
        context -> (accString + s"\n    $stepString")
      case ((context, accString), (ExpectError(errorType, errorPhase, detail, _), _)) =>
        val stepNumber = context.ExecQueryStepNr match {
          case Some(stepNr) => stepNr
          case None => throw new IllegalStateException(s"no query found to check for thrown error $detail")
        }
        //todo: check for errorType and detail (when corresponding errors exist in CAPS like SyntaxError, TypeError, ParameterMissing, ...)
        //todo: maybe check if they get imported? (or modify specificNamings case class with optional parameter
        val stepString =
        s"""
           |     val errorMessage$stepNumber  = an[Exception] shouldBe thrownBy{result$stepNumber}
           """.stripMargin
        context -> (accString + s"\n    $stepString")
      case ((context, accString), (SideEffects(expected, _), _)) =>
        val relevantSideEffects = expected.v.filter(_._2 > 0)
        val stepString = if (checkSideEffects) {
          val contextStep = context.graphStateStepNr match {
            case Some(v) => v
            case None => throw new IllegalStateException(s"no graph state found to check side effects")
          }
          s"""
             |    val afterState$contextStep = SideEffectOps.measureState(TCKGraph($graphFactoryName,graph))
             |    (beforeState$contextStep diff afterState$contextStep) shouldEqual ${diffToCreateString(expected)}
           """.stripMargin
        }
        else if (relevantSideEffects.nonEmpty) s"fail //due to ${relevantSideEffects.mkString(" ")} sideEffects expected"
        else ""
        context -> (accString + s"\n    $stepString")
      case ((context, accString), _) => context -> accString
    }._2
  }

  private def alignString(query: String, tabsNumber: Int = 3): String = {
    val tabs = (0 to tabsNumber).foldLeft("") { case (acc, _) => acc + "\t" }
    query.replaceAllLiterally("\n", s"\n$tabs")
  }
}
