package org.opencypher.spark.testing

import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.TckToCypherConverter.tckValueToCypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherList => OKAPICypherList, CypherMap => OKAPICypherMap, CypherNull => OKAPICypherNull, CypherString => OKAPICypherString, CypherValue => OKAPICypherValue}
import org.opencypher.tools.tck.values.{Backward => TCKBackward, CypherBoolean => TCKCypherBoolean, CypherFloat => TCKCypherFloat, CypherInteger => TCKCypherInteger, CypherList => TCKCypherList, CypherNode => TCKCypherNode, CypherNull => TCKCypherNull, CypherOrderedList => TCKCypherOrderedList, CypherPath => TCKCypherPath, CypherProperty => TCKCypherProperty, CypherPropertyMap => TCKCypherPropertyMap, CypherRelationship => TCKCypherRelationship, CypherString => TCKCypherString, CypherUnorderedList => TCKUnorderedList, CypherValue => TCKCypherValue, Forward => TCKForward}



object CreatetStringGenerator {
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

  def escapeString(s: String): String = {
    s""" "${StringEscapeUtils.escapeJava(s)}" """
  }

}
