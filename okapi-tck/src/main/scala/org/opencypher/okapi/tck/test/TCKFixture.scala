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

import java.time.format.DateTimeFormatter
import java.util.Objects

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.impl.exception.TypingException
import org.opencypher.okapi.tck.test.TCKFixture._
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory}
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.constants.{TCKErrorDetails, TCKErrorPhases, TCKErrorTypes}
import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, CypherString => TCKCypherString, CypherList => TCKCypherList, CypherOrderedList => TCKCypherOrderedList,
  CypherNode => TCKCypherNode, CypherRelationship => TCKCypherRelationship,
  CypherInteger => TCKCypherInteger, CypherFloat => TCKCypherFloat, CypherBoolean => TCKCypherBoolean, CypherProperty => TCKCypherProperty,
  CypherPropertyMap => TCKCypherPropertyMap, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath}
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source
import scala.util.{Failure, Success, Try}

// needs to be a val because of a TCK bug (scenarios can only be read once)
object TCKFixture {
  // TODO: enable flaky test once new TCk release is there
  val scenarios: Seq[Scenario] = CypherTCK.allTckScenarios.filterNot(_.name == "Limit to two hits")

  def printAll(): Unit = scenarios
    .enumerateScenarioOutlines
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

case class TCKGraph[C <: CypherSession](testGraphFactory: CypherTestGraphFactory[C], graph: PropertyGraph)
  (implicit OKAPI: C) extends Graph {

  override def execute(query: String, params: Map[String, TCKCypherValue], queryType: QueryType): (Graph, Result) = {
    queryType match {
      case InitQuery =>
        val propertyGraph = testGraphFactory(CreateGraphFactory(query, params.mapValues(TckToCypherConverter.tckValueToCypherValue)))
        copy(graph = propertyGraph) -> CypherValueRecords.empty
      case SideEffectQuery =>
        // this one is tricky, not sure how can do it without Cypher
        this -> CypherValueRecords.empty
      case ExecQuery =>
        // mapValues is lazy, so we force it for debug purposes
        val result = Try(graph.cypher(query, params.mapValues(TckToCypherConverter.tckValueToCypherValue).view.force))
        result match {
          case Success(r) => this -> CypherToTCKConverter.convertToTckStrings(r.records)
          case Failure(e) =>
            val phase = TCKErrorPhases.RUNTIME // We have no way to detect errors during compile time yet
            e match {
              case _: TypingException => this ->
                ExecutionFailed(TCKErrorTypes.TYPE_ERROR, phase, TCKErrorDetails.INVALID_ARGUMENT_VALUE)
              case ex: NotImplementedException => throw new RuntimeException(s"Unsupported feature in $query", ex)
              case _ => throw new RuntimeException(s"Unknown engine failure for query: $query", e)
            }
        }
    }
  }
}

case class ScenariosFor(blacklist: Set[String]) {

  def whiteList = Table(
    "scenario",
    scenarios
      .enumerateScenarioOutlines
      .filterNot(s => blacklist.contains(s.toString()))
      : _*
  )

  def blackList = Table(
    "scenario",
    scenarios
      .enumerateScenarioOutlines
      .filter(s => blacklist.contains(s.toString()))
      : _*
  )

  def get(name: String): Seq[Scenario] = scenarios.filter(s => s.name == name)
  def getPrefix(prefix: String): Seq[Scenario] = scenarios.filter(s => s.name.startsWith(prefix))
}

object ScenariosFor {

  def apply(blacklistFiles: String*): ScenariosFor = {
    val blacklistIter = blacklistFiles.flatMap(Source.fromFile(_).getLines())
    val blacklistSet = blacklistIter.toSet

    lazy val errorMessage =
      s"Blacklist contains duplicate scenarios ${blacklistIter.groupBy(identity).filter(_._2.lengthCompare(1) > 0).keys.mkString("\n")}"
    assert(blacklistIter.lengthCompare(blacklistSet.size) == 0, errorMessage)
    ScenariosFor(blacklistSet)
  }
}

object Tags {

  object WhiteList extends Tag("WhiteList Scenario")

  object BlackList extends Tag("BlackList Scenario")

}

object TckToCypherConverter {
  def tckValueToCypherValue(cypherValue: TCKCypherValue): CypherValue = cypherValue match {
    case TCKCypherString(v) => CypherValue(v)
    case TCKCypherInteger(v) => CypherValue(v)
    case TCKCypherFloat(v) => CypherValue(v)
    case TCKCypherBoolean(v) => CypherValue(v)
    case TCKCypherProperty(key, value) => CypherMap(key -> tckValueToCypherValue(value))
    case TCKCypherPropertyMap(properties) => CypherMap(properties.mapValues(tckValueToCypherValue))
    case l: TCKCypherList => CypherList(l.elements.map(tckValueToCypherValue))
    case TCKCypherNull => CypherValue(null)
    case other =>
      throw NotImplementedException(s"Converting Cypher value $cypherValue of type `${other.getClass.getSimpleName}`")
  }
}

object CypherToTCKConverter {

  def convertToTckStrings(records: CypherRecords): StringRecords = {
    val header = records.logicalColumns.getOrElse(records.physicalColumns).toList
    val rows: List[Map[String, String]] = records.collect.map { cypherMap: CypherMap =>
      cypherMap.keys.map(k => k -> cypherMap(k).toTCKString).toMap
    }.toList
    StringRecords(header, rows)
  }

  implicit class RichTCKCypherValue(value: CypherValue) {
    def toTCKString: String = {
      value match {
        case CypherString(s) => s"'${escape(s)}'"
        case CypherList(l) => l.map(_.toTCKString).mkString("[", ", ", "]")
        case CypherMap(m) =>
          m.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"$k: ${v.toTCKString}" }
            .mkString("{", ", ", "}")
        case CypherRelationship(_, _, _, relType, props) =>
          s"[:$relType${
            if (props.isEmpty) ""
            else s" ${props.toTCKString}"
          }]"
        case CypherNode(_, labels, props) =>
          val labelString =
            if (labels.isEmpty) ""
            else labels.toSeq.sorted.mkString(":", ":", "")
          val propertyString = if (props.isEmpty) ""
          else s"${props.toTCKString}"
          Seq(labelString, propertyString)
            .filter(_.nonEmpty)
            .mkString("(", " ", ")")
        case CypherDate(date) =>
          s"'${DateTimeFormatter.ISO_DATE.format(date)}'"
        case CypherLocalDateTime(localDateTime) =>
          s"'$localDateTime'"
        case _ => Objects.toString(value)
      }
    }

    private def escape(str: String): String = {
      str.replaceAllLiterally("'", "\\'").replaceAllLiterally("\"", "\\\"")
    }
  }
}
