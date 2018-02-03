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
package org.opencypher.caps.api.value

import org.opencypher.caps.api.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.api.types._

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

object CypherValue {

  def apply(v: Any): CypherValue = {
    v match {
      case cv: CypherValue => cv
      case s: Seq[_] => s.map(CypherValue(_)).toList
      case m: Map[_, _] => m.map { case (k, cv) => k.toString -> CypherValue(cv) }
      case ji: Integer => ji.toLong
      case b: Boolean => b
      case l: Long => l
      case i: Int => i.toLong
      case d: Double => d
      case f: Float => f.toDouble
      case s: String => s
      case null => CypherNull
      case invalid =>
        throw IllegalArgumentException("A valid CypherValue", invalid)
    }
  }

  sealed trait CypherValue extends Any {
    def value: Any

    def getValue: Any = Option(value)

    def isNull: Boolean = value == null

    def as[V: ClassTag]: Option[V] = Option(cast[V])

    def cast[V: ClassTag]: V = {
      this match {
        case cv: V => cv
        case _ =>
          value match {
            case v: V => v
            case unsupported => throw UnsupportedOperationException(
              s"Cannot cast $unsupported of type ${unsupported.getClass.getSimpleName} to ${classTag[V].getClass.getSimpleName}")
          }
      }
    }

    //TODO: Adapt return type
    def toScala: Any = {
      this match {
        case l: CypherList => l.value.map(_.toScala)
        case m: CypherMap => m.value.map { case (k, v) => k -> v.toScala }
        case other => other.value
      }
    }

    //TODO: Adapt return type
    def toJava: Any = {
      this match {
        case l: CypherList => l.value.map(_.toJava).asJava
        case m: CypherMap => m.value.map { case (k, v) => k -> v.toJava }.asJava
        case other => other.value
      }
    }

    private[caps] def isOrContainsNull: Boolean = Option(value).nonEmpty || {
      this match {
        case l: CypherList => l.value.exists(_.isOrContainsNull)
        case m: CypherMap => m.value.valuesIterator.exists(_.isOrContainsNull)
        case _ => false
      }
    }

    def toCypherString: String = {
      this match {
        case CypherNull => "null"
        case s: CypherString => s"'${s.value}'"
        case l: CypherList => l.value.map(_.toCypherString).mkString("[", ", ", "]")
        case m: CypherMap =>
          m.value.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"$k: ${v.toCypherString}" }
            .mkString("{", ", ", "}")
        case r: CypherRelationship[_] =>
          s"[:${r.relType}${
            if (r.properties.value.isEmpty) ""
            else s" ${r.properties.toCypherString}"
          }]"
        case n: CypherNode[_] =>
          val labelString =
            if (n.labels.isEmpty) ""
            else n.labels.toSeq.sorted.mkString(":", ":", "")
          val propertyString = if (n.properties.value.isEmpty) ""
          else s"${n.properties.toCypherString}"
          Seq(labelString, propertyString)
            .filter(_.nonEmpty)
            .mkString("(", " ", ")")
        case _ => value.toString
      }
    }

    private[caps] def cypherType: CypherType = {
      val material = this match {
        case CypherNull => CTNull
        case _: CypherBoolean => CTBoolean
        case _: CypherFloat => CTFloat
        case _: CypherInteger => CTInteger
        case _: CypherString => CTString
        case _: CypherMap => CTMap
        case n: CypherNode[_] => CTNode(n.labels)
        case r: CypherRelationship[_] => CTRelationship(r.relType)
        case l: CypherList => CTList(l.value.map(_.cypherType).foldLeft[CypherType](CTVoid)(_.join(_)))
      }
      if (Option(value).isEmpty) {
        material.nullable
      } else {
        material
      }
    }

  }

  object CypherNull extends CypherValue {
    def value: Null = null
  }

  implicit class CypherString(val value: String) extends AnyVal with CypherValue

  implicit class CypherBoolean(val value: Boolean) extends AnyVal with CypherValue

  sealed trait CypherNumber extends Any with CypherValue

  implicit class CypherInteger(val value: Long) extends AnyVal with CypherNumber

  implicit class CypherFloat(val value: Double) extends AnyVal with CypherNumber

  implicit class CypherMap(val value: Map[String, CypherValue]) extends AnyVal with CypherValue {
    def keys: Set[String] = value.keySet

    def get(k: String): Option[CypherValue] = value.get(k).flatMap(v => Option(v))

    def apply(k: String): CypherValue = value.getOrElse(k, CypherNull)
  }

  object CypherMap {

    def apply(values: (String, Any)*): CypherMap = {
      values.map { case (k, v) => k -> CypherValue(v) }.toMap
    }

    val empty: CypherMap = Map.empty[String, CypherValue]
  }

  implicit class CypherList(val value: List[CypherValue]) extends AnyVal with CypherValue

  object CypherList {

    def apply(elem: Any*): CypherList = elem.map(CypherValue(_)).toList

    val empty: CypherList = List.empty[CypherValue]

    def unapplySeq(l: CypherList): Option[List[CypherValue]] =
      Option(l).map(_.value)
  }


  sealed trait CypherEntity[+Id] {
    def id: Id

    def properties: CypherMap
  }

  class CypherNode[+Id](val id: Id,
    val labels: Set[String] = Set.empty,
    val properties: CypherMap = CypherMap.empty)
    extends CypherEntity[Id]
      with CypherValue {
    override def value: CypherNode[Id] = this

    override def equals(that: Any): Boolean = {
      that match {
        case cn: CypherNode[_] if cn.id == id => true
        case _ => false
      }
    }

    override def hashCode: Int = id.hashCode
  }

  object CypherNode {

    def unapply[Id](n: CypherNode[Id]): Option[(Id, Set[String], CypherMap)] = {
      Some((n.id, n.labels, n.properties))
    }
  }

  class CypherRelationship[+Id](
    val id: Id,
    val source: Id,
    val target: Id,
    val relType: String,
    val properties: CypherMap = CypherMap.empty)
    extends CypherEntity[Id]
      with CypherValue {

    override def value: CypherRelationship[Id] = this

    override def equals(that: Any): Boolean = {
      that match {
        case cr: CypherRelationship[_] if cr.id == id => true
        case _ => false
      }
    }

    override def hashCode: Int = id.hashCode
  }

  object CypherRelationship {

    def unapply[Id](
      r: CypherRelationship[Id]): Option[(Id, Id, Id, String, CypherMap)] = {
      Some((r.id, r.source, r.target, r.relType, r.properties))
    }
  }

}
