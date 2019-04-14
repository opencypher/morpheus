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


import org.apache.commons.lang3.StringEscapeUtils
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.TckToCypherConverter.tckValueToCypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherList => OKAPICypherList, CypherMap => OKAPICypherMap, CypherNull => OKAPICypherNull, CypherString => OKAPICypherString, CypherValue => OKAPICypherValue}
import org.opencypher.tools.tck.SideEffectOps.Diff
import org.opencypher.tools.tck.values.{Backward => TCKBackward, CypherBoolean => TCKCypherBoolean, CypherFloat => TCKCypherFloat, CypherInteger => TCKCypherInteger, CypherList => TCKCypherList, CypherNode => TCKCypherNode, CypherNull => TCKCypherNull, CypherOrderedList => TCKCypherOrderedList, CypherPath => TCKCypherPath, CypherProperty => TCKCypherProperty, CypherPropertyMap => TCKCypherPropertyMap, CypherRelationship => TCKCypherRelationship, CypherString => TCKCypherString, CypherUnorderedList => TCKUnorderedList, CypherValue => TCKCypherValue, Forward => TCKForward}

object CreateStringGenerator {
  def tckCypherValueToCreateString(value: TCKCypherValue): String = {
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
      case TCKCypherNode(labels, properties) =>
        val labelsString = if (labels.isEmpty) "" else s"Set(${labels.map(escapeString).mkString(",")})"
        val propertyString = if (properties.properties.isEmpty) "" else s"${tckCypherValueToCreateString(properties)}"
        val separatorString =
          if (labels.isEmpty && properties.properties.isEmpty) ""
          else if (labels.isEmpty) "properties = "
          else if (properties.properties.isEmpty) ""
          else ", "

        s"TCKCypherNode($labelsString$separatorString$propertyString)"
      case TCKCypherRelationship(typ, properties) =>
        val propertyString = if (properties.properties.isEmpty) "" else s", ${tckCypherValueToCreateString(properties)}"
        s"TCKCypherRelationship(${escapeString(typ)}$propertyString)"
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

  def cypherValueToCreateString(value: OKAPICypherValue): String = {
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

  def tckCypherMapToTCKCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, cypherValue) => escapeString(key) -> tckCypherValueToCreateString(cypherValue)
    }
    s"Map(${mapElementsString.mkString(",")})"
  }

  def tckCypherMapToOkapiCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, tckCypherValue) => escapeString(key) -> cypherValueToCreateString(tckValueToCypherValue(tckCypherValue))
    }
    s"CypherMap(${mapElementsString.mkString(",")})"
  }

  def diffToCreateString(diff: Diff): String = {
    val diffs = diff.v.map { case (s, i) => escapeString(s) -> i }
    s"Diff(Map(${diffs.mkString(",")}}))"
  }

  def escapeString(s: String): String = {
    s""" "${StringEscapeUtils.escapeJava(s)}" """
  }

}
