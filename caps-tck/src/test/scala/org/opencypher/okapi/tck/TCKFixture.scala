/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.okapi.tck

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherList => CAPSCypherList, CypherMap => CAPSCypherMap, CypherValue => CAPSCypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.impl.typer.exception.TypingException
import org.opencypher.okapi.ir.test.support.creation.TestGraphFactory
import org.opencypher.okapi.tck.TCKFixture._
import org.opencypher.okapi.test.support.creation.propertygraph.Neo4jPropertyGraphFactory
import org.opencypher.tools.tck.api.{ExecutionFailed, _}
import org.opencypher.tools.tck.constants.{TCKErrorDetails, TCKErrorPhases, TCKErrorTypes}
import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, _}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source
import scala.util.{Failure, Success, Try}

// needs to be a val because of a TCK bug (scenarios can only be read once)
object TCKFixture {
  // TODO: enable flaky test once new TCk release is there
  val scenarios: Seq[Scenario] = CypherTCK.allTckScenarios.filterNot(_.name == "Limit to two hits")

  def printAll(): Unit = scenarios
    .map(s => s"""Feature "${s.featureName}": Scenario "${s.name}"""")
    .distinct
    .sorted
    .foreach(println)

  implicit class Scenarios(scenarios: Seq[Scenario]) {
    /**
      * Scenario outlines are parameterised scenarios that all have the same name, but different parameters.
      * Because test names need to be unique, we enumerate these scenarios and put the enumeration into the
      * scenario name to make those names unique.
      */
    def enumerateScenarioOutlines: Seq[Scenario] = {
      scenarios.groupBy(_.toString).flatMap { case (_, nameCollisionGroup) =>
        if (nameCollisionGroup.size <= 1) {
          nameCollisionGroup
        } else {
          nameCollisionGroup.zipWithIndex.map { case (groupScenario, index) =>
            groupScenario.copy(name = s"${groupScenario.name} #${index + 1}")
          }
        }
      }
    }.toSeq
  }
}

case class TCKGraph[C <: CypherSession](testGraphFactory: TestGraphFactory[C], graph: PropertyGraph)(implicit caps: C) extends Graph {

  override def execute(query: String, params: Map[String, TCKCypherValue], queryType: QueryType): (Graph, Result) = {
    queryType match {
      case InitQuery =>
        val propertyGraph = testGraphFactory(Neo4jPropertyGraphFactory(query, params.mapValues(tckValueToCAPSValue)))
        copy(graph = propertyGraph) -> CypherValueRecords.empty
      case SideEffectQuery =>
        // this one is tricky, not sure how can do it without Cypher
        this -> CypherValueRecords.empty
      case ExecQuery =>
        // mapValues is lazy, so we force it for debug purposes
        val capsResult = Try(graph.cypher(query, params.mapValues(tckValueToCAPSValue).view.force))
        capsResult match {
          case Success(r) => this -> convertToTckStrings(r.records)
          case Failure(e) =>
            val phase = TCKErrorPhases.RUNTIME // We have no way to detect errors during compile time yet
            e match {
              case t: TypingException => this ->
                ExecutionFailed(TCKErrorTypes.TYPE_ERROR, phase, TCKErrorDetails.INVALID_ARGUMENT_VALUE)
            }
        }
    }
  }

  private def convertToTckStrings(records: CypherRecords): StringRecords = {
    val header = records.columns.toList
    val rows: List[Map[String, String]] = records.collect.map { cypherMap: CAPSCypherMap =>
      cypherMap.keys.map(k => k -> cypherMap(k).toCypherString).toMap
    }.toList
    StringRecords(header, rows)
  }

  private def tckValueToCAPSValue(cypherValue: TCKCypherValue): CAPSCypherValue = cypherValue match {
    case CypherString(v) => CypherValue(v)
    case CypherInteger(v) => CypherValue(v)
    case CypherFloat(v) => CypherValue(v)
    case CypherBoolean(v) => CypherValue(v)
    case CypherProperty(key, value) => CAPSCypherMap(key -> tckValueToCAPSValue(value))
    case CypherPropertyMap(properties) => CAPSCypherMap(properties.mapValues(tckValueToCAPSValue))
    case l: CypherList => CAPSCypherList(l.elements.map(tckValueToCAPSValue))
    case CypherNull => CypherValue(null)
    case other =>
      throw NotImplementedException(s"Converting Cypher value $cypherValue of type `${other.getClass.getSimpleName}`")
  }
}

case class ScenariosFor(engine: String)  {

  def whiteList = Table(
    "scenario",
    scenarios.filterNot { s =>
      blacklist.contains(s.toString())
    }.enumerateScenarioOutlines: _*
  )

  def blackList = Table(
    "scenario",
    scenarios.filter { s =>
      blacklist.contains(s.toString())
    }.enumerateScenarioOutlines: _*
  )

  def get(name: String): Seq[Scenario] = scenarios.filter(s => s.name == name)

  private lazy val blacklist: Set[String] = {
    val blacklistIter = Source.fromFile(getClass.getResource(s"scenario_blacklist_$engine").toURI).getLines().toSeq
    val blacklistSet = blacklistIter.toSet

    lazy val errorMessage =
      s"Blacklist contains duplicate scenarios ${blacklistIter.groupBy(identity).filter(_._2.lengthCompare(1) > 0).keys.mkString("\n")}"
    assert(blacklistIter.lengthCompare(blacklistSet.size) == 0, errorMessage)
    blacklistSet
  }
}
