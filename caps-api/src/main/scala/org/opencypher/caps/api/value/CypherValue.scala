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

import java.util.Objects

import org.opencypher.caps.api.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.api.types._

import scala.reflect.{ClassTag, classTag}
import scala.util.Try

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

  def unapply(cv: CypherValue): Option[Any] = {
    Option(cv).flatMap(v => Option(v.value))
  }

  sealed trait CypherValue extends Any {
    def value: Any

    def getValue: Option[Any]

    def unwrap: Any

    def isNull: Boolean = Objects.isNull(value)

    def as[V: ClassTag]: Option[V] = Try(cast[V]).toOption

    def cast[V: ClassTag]: V = {
      this match {
        case cv: V => cv
        case _ =>
          value match {
            case v: V => v
            case _ =>
                throw UnsupportedOperationException(
                  s"Cannot cast $value of type ${value.getClass.getSimpleName} to ${classTag[V].runtimeClass.getSimpleName}")
          }
      }
    }

    override def toString: String = Objects.toString(unwrap)

    override def hashCode: Int = Objects.hashCode(unwrap)

    override def equals(other: Any): Boolean = {
      other match {
        case cv: CypherValue => Objects.equals(unwrap, cv.unwrap)
        case _ => false
      }
    }

    def toCypherString: String = {
      this match {
        case CypherString(s) => s"'$s'"
        case CypherList(l) => l.map(_.toCypherString).mkString("[", ", ", "]")
        case CypherMap(m) =>
          m.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"$k: ${v.toCypherString}" }
            .mkString("{", ", ", "}")
        case CypherRelationship(_, _, _, relType, CypherMap(properties)) =>
          s"[:$relType${
            if (properties.isEmpty) ""
            else s" ${properties.toCypherString}"
          }]"
        case CypherNode(_, labels, CypherMap(properties)) =>
          val labelString =
            if (labels.isEmpty) ""
            else labels.toSeq.sorted.mkString(":", ":", "")
          val propertyString = if (properties.isEmpty) ""
          else s"${properties.toCypherString}"
          Seq(labelString, propertyString)
            .filter(_.nonEmpty)
            .mkString("(", " ", ")")
        case _ => Objects.toString(value)
      }
    }

    private[caps] def cypherType: CypherType = {
      this match {
        case CypherNull => CTNull
        case CypherBoolean(_) => CTBoolean
        case CypherFloat(_) => CTFloat
        case CypherInteger(_) => CTInteger
        case CypherString(_) => CTString
        case CypherMap(_) => CTMap
        case CypherNode(_, labels, _) => CTNode(labels)
        case CypherRelationship(_, _, _, relType, _) => CTRelationship(relType)
        case CypherList(l) => CTList(l.map(_.cypherType).foldLeft[CypherType](CTVoid)(_.join(_)))
      }
    }

    private[caps] def isOrContainsNull: Boolean = isNull || {
      this match {
        case l: CypherList => l.value.exists(_.isOrContainsNull)
        case m: CypherMap => m.value.valuesIterator.exists(_.isOrContainsNull)
        case _ => false
      }
    }

  }

  object CypherNull extends CypherValue {
    override def value: Null = null

    override def unwrap: Null = value

    override def getValue: Option[Any] = None
  }

  implicit class CypherString(val value: String) extends AnyVal with PrimitiveCypherValue[String]

  implicit class CypherBoolean(val value: Boolean) extends AnyVal with PrimitiveCypherValue[Boolean]

  sealed trait CypherNumber[+V] extends Any with PrimitiveCypherValue[V]

  implicit class CypherInteger(val value: Long) extends AnyVal with CypherNumber[Long]

  implicit class CypherFloat(val value: Double) extends AnyVal with CypherNumber[Double]

  implicit class CypherMap(val value: Map[String, CypherValue]) extends AnyVal with MaterialCypherValue[Map[String, CypherValue]] {
    override def unwrap: Map[String, Any] = value.map { case (k, v) => k -> v.unwrap }

    def keys: Set[String] = value.keySet

    def get(k: String): Option[CypherValue] = value.get(k)

    def apply(k: String): CypherValue = value.getOrElse(k, CypherNull)

    def ++(other: CypherMap): CypherMap = value ++ other.value
  }

  object CypherMap extends UnapplyValue[Map[String, CypherValue], CypherMap] {
    def apply(values: (String, Any)*): CypherMap = {
      values.map { case (k, v) => k -> CypherValue(v) }.toMap
    }

    val empty: CypherMap = Map.empty[String, CypherValue]

  }

  implicit class CypherList(val value: List[CypherValue]) extends AnyVal with MaterialCypherValue[List[CypherValue]] {
    override def unwrap: List[Any] = value.map(_.unwrap)
  }

  object CypherList extends UnapplyValue[List[CypherValue], CypherList] {
    def apply(elem: Any*): CypherList = elem.map(CypherValue(_)).toList

    val empty: CypherList = List.empty[CypherValue]
  }

  sealed trait CypherEntity[+Id] {
    def id: Id

    def properties: CypherMap
  }

  class CypherNode[+Id](val id: Id,
    val labels: Set[String] = Set.empty,
    val properties: CypherMap = CypherMap.empty)
    extends CypherEntity[Id]
      with MaterialCypherValue[CypherNode[Id]] {
    override def value: CypherNode[Id] = this

    override def unwrap: CypherNode[Id] = this

    override def equals(that: Any): Boolean = {
      that match {
        case CypherNode(otherId, _, _) => Objects.equals(id, otherId)
        case _ => false
      }
    }

    override def hashCode: Int = id.hashCode

    override def toString = s"${this.getClass.getSimpleName}($id, $labels, $properties)"
  }

  object CypherNode {

    def unapply[Id](n: CypherNode[Id]): Option[(Id, Set[String], CypherMap)] = {
      Option(n).map(node => (node.id, node.labels, node.properties))
    }
  }

  class CypherRelationship[+Id](
    val id: Id,
    val source: Id,
    val target: Id,
    val relType: String,
    val properties: CypherMap = CypherMap.empty)
    extends CypherEntity[Id]
      with MaterialCypherValue[CypherRelationship[Id]] {

    override def value: CypherRelationship[Id] = this

    override def unwrap: CypherRelationship[Id] = this

    override def equals(that: Any): Boolean = {
      that match {
        case CypherRelationship(otherId, _, _, _, _) => Objects.equals(id, otherId)
        case _ => false
      }
    }

    override def hashCode: Int = id.hashCode

    override def toString = s"${this.getClass.getSimpleName}($id, $source, $target, $relType, $properties)"
  }

  object CypherRelationship {

    def unapply[Id](r: CypherRelationship[Id]): Option[(Id, Id, Id, String, CypherMap)] = {
      Option(r).map(rel => (rel.id, rel.source, rel.target, rel.relType, rel.properties))
    }
  }

  trait MaterialCypherValue[+T] extends Any with CypherValue {
    override def value: T

    override def getValue: Option[T] = Option(value)
  }

  /**
    * A primitive Cypher value is one that does not contain any other Cypher values.
    */
  trait PrimitiveCypherValue[+T] extends Any with MaterialCypherValue[T] {
    override def unwrap: T = value
  }

  abstract class UnapplyValue[V, CV <: MaterialCypherValue[V]] {
    def unapply(v: CV): Option[V] = Option(v).flatMap(_.getValue)
  }

  object CypherString extends UnapplyValue[String, CypherString]

  object CypherBoolean extends UnapplyValue[Boolean, CypherBoolean]

  object CypherInteger extends UnapplyValue[Long, CypherInteger]

  object CypherFloat extends UnapplyValue[Double, CypherFloat]

}
