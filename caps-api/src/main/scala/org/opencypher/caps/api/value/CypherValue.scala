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

import org.opencypher.caps.api.exception._

import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import scala.util.hashing.MurmurHash3

object CypherValue {

  /**
    * Converts a Scala/Java value to a compatible Cypher value, fails if the conversion is not supported.
    *
    * @param v value to convert
    * @return compatible CypherValue
    */
  def apply(v: Any): CypherValue = {
    def seqToCypherList(s: Seq[_]): CypherList = s.map(CypherValue(_)).toList

    v match {
      case cv: CypherValue => cv
      case null => CypherNull
      case jb: java.lang.Byte => jb.toLong
      case js: java.lang.Short => js.toLong
      case ji: java.lang.Integer => ji.toLong
      case jl: java.lang.Long => jl.toLong
      case jf: java.lang.Float => jf.toDouble
      case jd: java.lang.Double => jd.toDouble
      case js: java.lang.String => js.toString
      case jb: java.lang.Boolean => jb.booleanValue
      case jl: java.util.List[_] => seqToCypherList(jl.toArray)
      case a: Array[_] => seqToCypherList(a)
      case s: Seq[_] => seqToCypherList(s)
      case m: Map[_, _] => m.map { case (k, cv) => k.toString -> CypherValue(cv) }
      case b: Byte => b.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toDouble
      case d: Double => d
      case b: Boolean => b
      case invalid =>
        throw IllegalArgumentException(
          "a value that can be converted to a Cypher value", s"$invalid of type ${invalid.getClass.getName}")
    }
  }

  /**
    * Attempts to extract the wrapped value from a CypherValue.
    *
    * @param cv CypherValue to extract from
    * @return none or some extracted value.
    */
  def unapply(cv: CypherValue): Option[Any] = {
    Option(cv).flatMap(v => Option(v.value))
  }

  /**
    * CypherValue is a wrapper for Scala/Java classes that represent valid Cypher values.
    */
  sealed trait CypherValue extends Any {
    /**
      * @return wrapped value
      */
    def value: Any

    /**
      * @return null-safe version of [[value]]
      */
    def getValue: Option[Any]

    /**
      * @return unwraps the Cypher value into Scala/Java structures. Unlike [[value]] this is done recursively for the
      *         Cypher values stored inside of maps and lists.
      */
    def unwrap: Any

    /**
      * @return true iff the stored value is null.
      */
    def isNull: Boolean = Objects.isNull(value)

    /**
      * Safe version of [[cast]]
      */
    def as[V: ClassTag]: Option[V] = Try(cast[V]).toOption

    /**
      * Attempts to cast the Cypher value to [[V]], fails when this is not supported.
      */
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

    /**
      * String of the Scala representation of this value.
      */
    override def toString: String = Objects.toString(unwrap)

    /**
      * Hash code of the Scala representation.
      */
    override def hashCode: Int = Objects.hashCode(unwrap)

    /**
      * Structural comparison of the Scala representation.
      *
      * This is NOT Cypher equality or equivalence.
      */
    override def equals(other: Any): Boolean = {
      other match {
        case cv: CypherValue => Objects.equals(unwrap, cv.unwrap)
        case _ => false
      }
    }

    /**
      * A Cypher string representation. For more information about the exact format of these, please refer to
      * [[https://github.com/opencypher/openCypher/tree/master/tck#format-of-the-expected-results the openCypher TCK]].
      */
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

  sealed trait CypherEntity[+Id] extends Product with MaterialCypherValue[CypherEntity[Id]] {
    def id: Id

    def properties: CypherMap

    override def hashCode: Int = {
      MurmurHash3.orderedHash(productIterator, MurmurHash3.stringHash(productPrefix))
    }

    override def equals(other: Any): Boolean = other match {
      case that: CypherEntity[_] =>
        (that canEqual this) && haveEqualValues(this.productIterator, that.productIterator)
      case _ =>
        false
    }

    protected def haveEqualValues(a: Iterator[Any], b: Iterator[Any]): Boolean = {
      while (a.hasNext && b.hasNext) {
        if (a.next != b.next) return false
      }
      a.hasNext == b.hasNext
    }

    override def productPrefix: String = getClass.getSimpleName

    override def toString = s"$productPrefix(${productIterator.mkString(", ")})"
  }

  trait CypherNode[+Id] extends CypherEntity[Id] with MaterialCypherValue[CypherNode[Id]] {
    def labels: Set[String] = Set.empty

    override def value: CypherNode[Id] = this

    override def unwrap: CypherNode[Id] = this

    override def productArity: Int = 3

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => labels
      case 2 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[CypherNode[_]]
  }

  object CypherNode {

    def unapply[Id](n: CypherNode[Id]): Option[(Id, Set[String], CypherMap)] = {
      Option(n).map(node => (node.id, node.labels, node.properties))
    }
  }

  trait CypherRelationship[+Id] extends CypherEntity[Id] with MaterialCypherValue[CypherRelationship[Id]] with Product {
    def source: Id

    def target: Id

    def relType: String

    override def value: CypherRelationship[Id] = this

    override def unwrap: CypherRelationship[Id] = this

    override def productArity: Int = 5

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => source
      case 2 => target
      case 3 => relType
      case 4 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[CypherRelationship[_]]
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
