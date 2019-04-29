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
package org.opencypher.okapi.api.value

import java.math.MathContext
import java.util.Objects

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.Element._
import org.opencypher.okapi.api.value.CypherValue.Node._
import org.opencypher.okapi.api.value.CypherValue.Relationship._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.temporal.Duration
import ujson._

import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import scala.util.hashing.MurmurHash3

object CypherValue {

  /**
    * Converts a Scala/Java value to a compatible Cypher value, fails if the conversion is not supported.
    *
    * @param v               value to convert
    * @param customConverter additional conversion rules
    * @return compatible CypherValue
    */
  def apply(v: Any)(implicit customConverter: CypherValueConverter = NoopCypherValueConverter): CypherValue = {

    def seqToCypherList(s: Seq[_]): CypherList = s.map(CypherValue(_)).toList

    customConverter.convert(v).getOrElse(
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
        case dt: java.sql.Date => dt.toLocalDate
        case ts: java.sql.Timestamp => ts.toLocalDateTime
        case ld: java.time.LocalDate => ld
        case ldt: java.time.LocalDateTime => ldt
        case du: Duration => du
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
        case b: BigDecimal => b
        case b: java.math.BigDecimal => BigDecimal(b)
        case invalid =>
          throw IllegalArgumentException(
            "a value that can be converted to a Cypher value", s"$invalid of type ${invalid.getClass.getName}")
      })
  }

  /**
    * Trait to inject additional CypherValue conversion rules
    */
  trait CypherValueConverter {
    def convert(v: Any): Option[CypherValue]
  }
  object NoopCypherValueConverter extends CypherValueConverter {
    override def convert(v: Any): Option[CypherValue] = None
  }

  /**
    * Converts a Scala/Java value to a compatible Cypher value.
    *
    * @param v value to convert
    * @return Some compatible CypherValue or None
    */
  def get(v: Any): Option[CypherValue] = {
    Try(apply(v)).toOption
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

  object Format {
    /**
      * Formats a given value to its String representation.
      */
    implicit def defaultValueFormatter(value: Any): String = value match {
      case s: Seq[_] => s.map(defaultValueFormatter).mkString
      case a: Array[_] => a.map(defaultValueFormatter).mkString
      case b: Byte => "%02X".format(b)
      case other => Objects.toString(other)
    }
  }

  /**
    * CypherValue is a wrapper for Scala/Java classes that represent valid Cypher values.
    */
  sealed trait CypherValue extends Any {
    /**
      * @return wrapped value
      */
    def value: Any

    def cypherType: CypherType

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
    def as[V: ClassTag]: Option[V] = {
      this match {
        case cv: V => Some(cv)
        case _ =>
          value match {
            case v: V => Some(v)
            case _ => None
          }
      }
    }

    /**
      * Attempts to cast the Cypher value to `V`, fails when this is not supported.
      */
    def cast[V: ClassTag]: V = as[V].getOrElse(throw UnsupportedOperationException(
      s"Cannot cast $value of type ${value.getClass.getSimpleName} to ${classTag[V].runtimeClass.getSimpleName}"))


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
    def toCypherString()(implicit formatValue: Any => String): String = {
      this match {
        case CypherString(s) => s"'${escape(s)}'"
        case CypherList(l) => l.map(_.toCypherString).mkString("[", ", ", "]")
        case CypherMap(m) =>
          m.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"`${escape(k)}`: ${v.toCypherString}" }
            .mkString("{", ", ", "}")
        case Relationship(_, _, _, relType, props) =>
          s"[:`${escape(relType)}`${
            if (props.isEmpty) ""
            else s" ${props.toCypherString}"
          }]"
        case Node(_, labels, props) =>
          val labelString =
            if (labels.isEmpty) ""
            else labels.toSeq.sorted.map(escape).mkString(":`", "`:`", "`")
          val propertyString = if (props.isEmpty) ""
          else s"${props.toCypherString}"
          Seq(labelString, propertyString)
            .filter(_.nonEmpty)
            .mkString("(", " ", ")")
        case other => formatValue(other)
      }
    }

    def toJson()(implicit formatValue: Any => String): Value = {
      this match {
        case CypherNull => Null
        case CypherString(s) => Str(s)
        case CypherList(l) => l.map(_.toJson)
        case CypherMap(m) => m.mapValues(_.toJson).toSeq.sortBy(_._1)
        case Relationship(id, startId, endId, relType, properties) =>
          Obj(
            idJsonKey -> Str(formatValue(id)),
            typeJsonKey -> Str(relType),
            startIdJsonKey -> Str(formatValue(startId)),
            endIdJsonKey -> Str(formatValue(endId)),
            propertiesJsonKey -> properties.toJson
          )
        case Node(id, labels, properties) =>
          Obj(
            idJsonKey -> Str(formatValue(id)),
            labelsJsonKey -> labels.toSeq.sorted.map(Str),
            propertiesJsonKey -> properties.toJson
          )
        case CypherFloat(d) => Num(d)
        case CypherInteger(l) => Str(l.toString) // `Num` would lose precision
        case CypherBoolean(b) => Bool(b)
        case CypherBigDecimal(b) => Obj(
          "type" -> Str("BigDecimal"),
          "scale" -> Num(b.bigDecimal.scale()),
          "precision" -> Num(b.bigDecimal.precision())
        )
        case other => Str(formatValue(other.value))
      }
    }

    private def escape(str: String): String = {
      str
        .replaceAllLiterally("""\""", """\\""")
        .replaceAllLiterally("'", "\\'")
        .replaceAllLiterally("\"", "\\\"")
    }

    private[opencypher] def isOrContainsNull: Boolean = isNull || {
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

    override def cypherType: CypherType = CTNull
  }

  implicit class CypherString(val value: String) extends AnyVal with PrimitiveCypherValue[String] {
    override def cypherType: CypherType = CTString
  }

  implicit class CypherBoolean(val value: Boolean) extends AnyVal with PrimitiveCypherValue[Boolean] {
    override def cypherType: CypherType = if (value) CTTrue else CTFalse
  }

  sealed trait CypherNumber[+V] extends Any with PrimitiveCypherValue[V]

  implicit class CypherInteger(val value: Long) extends AnyVal with CypherNumber[Long] {
    override def cypherType: CypherType = CTInteger
  }

  implicit class CypherFloat(val value: Double) extends AnyVal with CypherNumber[Double] {
    override def cypherType: CypherType = CTFloat
  }

  implicit class CypherBigDecimal(val value: BigDecimal) extends AnyVal with CypherNumber[BigDecimal] {
    override def cypherType: CypherType = CTBigDecimal(value.precision, value.scale)
  }

  implicit class CypherLocalDateTime(val value: java.time.LocalDateTime) extends AnyVal with MaterialCypherValue[java.time.LocalDateTime] {
    override def unwrap: Any = value
    override def cypherType: CypherType = CTLocalDateTime
  }

  implicit class CypherDate(val value: java.time.LocalDate) extends AnyVal with MaterialCypherValue[java.time.LocalDate] {
    override def unwrap: Any = value
    override def cypherType: CypherType = CTDate
  }

  implicit class CypherDuration(val value: Duration) extends AnyVal with MaterialCypherValue[Duration] {
    override def unwrap: Any = value
    override def cypherType: CypherType = CTDuration
  }

  implicit class CypherMap(val value: Map[String, CypherValue]) extends AnyVal with MaterialCypherValue[Map[String, CypherValue]] {
    override def unwrap: Map[String, Any] = value.map { case (k, v) => k -> v.unwrap }

    def isEmpty: Boolean = value.isEmpty

    def keys: Set[String] = value.keySet

    def get(k: String): Option[CypherValue] = value.get(k)

    def getOrElse(k: String, default: CypherValue = CypherNull): CypherValue = value.getOrElse(k, default)

    def apply(k: String): CypherValue = value.getOrElse(k, CypherNull)

    def ++(other: CypherMap): CypherMap = value ++ other.value

    def updated(k: String, v: CypherValue): CypherMap = value.updated(k, v)

    override def cypherType: CypherType = CTMap(value.mapValues(_.cypherType))

  }

  object CypherMap extends UnapplyValue[Map[String, CypherValue], CypherMap] {
    def apply(values: (String, Any)*): CypherMap = {
      values.map { case (k, v) => k -> CypherValue(v) }.toMap
    }

    val empty: CypherMap = Map.empty[String, CypherValue]

  }

  implicit class CypherList(val value: List[CypherValue]) extends AnyVal with MaterialCypherValue[List[CypherValue]] {
    override def unwrap: List[Any] = value.map(_.unwrap)
    override def cypherType: CypherType = CTList(CTUnion(value.map(_.cypherType): _*))
  }

  object CypherList extends UnapplyValue[List[CypherValue], CypherList] {
    def apply(elem: Any*): CypherList = elem.map(CypherValue(_)).toList

    val empty: CypherList = List.empty[CypherValue]
  }

  trait Element[Id] extends Product with MaterialCypherValue[Element[Id]] {
    type I <: Element[Id]

    def id: Id

    def properties: CypherMap

    override def hashCode: Int = {
      MurmurHash3.orderedHash(productIterator, MurmurHash3.stringHash(productPrefix))
    }

    override def equals(other: Any): Boolean = other match {
      case that: Element[_] =>
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

    def withProperty(key: String, value: CypherValue): I

  }

  object Element {

    val idJsonKey: String = "id"
    val propertiesJsonKey: String = "properties"

  }

  trait Node[Id] extends Element[Id] with MaterialCypherValue[Node[Id]] {
    override type I <: Node[Id]

    def id: Id

    def labels: Set[String]

    override def value: Node[Id] = this

    override def cypherType: CypherType = CTNode(labels)

    override def unwrap: Node[Id] = this

    override def productArity: Int = 3

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => labels
      case 2 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[Node[_]]

    def copy(id: Id = id, labels: Set[String] = labels, properties: CypherMap = properties): I

    def withLabel(label: String): I = {
      copy(labels = labels + label)
    }

    override def withProperty(key: String, value: CypherValue): I = {
      copy(properties = properties.value.updated(key, value))
    }

  }

  object Node {

    val labelsJsonKey: String = "labels"

    def unapply[Id](n: Node[Id]): Option[(Id, Set[String], CypherMap)] = {
      Option(n).map(node => (node.id, node.labels, node.properties))
    }

  }

  trait Relationship[Id] extends Element[Id] with MaterialCypherValue[Relationship[Id]] with Product {

    override type I <: Relationship[Id]

    def id: Id

    def startId: Id

    def endId: Id

    def relType: String

    override def value: Relationship[Id] = this

    override def cypherType: CypherType = CTRelationship(Set(relType))

    override def unwrap: Relationship[Id] = this

    override def productArity: Int = 5

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => startId
      case 2 => endId
      case 3 => relType
      case 4 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[Relationship[_]]

    def copy(
      id: Id = id,
      source: Id = startId,
      target: Id = endId,
      relType: String = relType,
      properties: CypherMap = properties): I

    def withType(relType: String): I = {
      copy(relType = relType)
    }

    override def withProperty(key: String, value: CypherValue): I = {
      copy(properties = properties.value.updated(key, value))
    }

  }

  object Relationship {

    val typeJsonKey: String = "type"
    val startIdJsonKey: String = "startId"
    val endIdJsonKey: String = "endId"

    def unapply[Id](r: Relationship[Id]): Option[(Id, Id, Id, String, CypherMap)] = {
      Option(r).map(rel => (rel.id, rel.startId, rel.endId, rel.relType, rel.properties))
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

  object CypherBigDecimal extends UnapplyValue[BigDecimal, CypherBigDecimal] {

    def apply(v: Any, precision: Int, scale: Int): CypherBigDecimal = {

      val context = new MathContext(precision)

      val bigDecimal = v match {
        case i: Int => BigDecimal(i, context)
        case l: Long => BigDecimal(l, context)
        case f: Float => BigDecimal(f.toDouble, context)
        case d: Double => BigDecimal(d, context)
        case s: String => BigDecimal(s, context)
      }

      bigDecimal.setScale(scale)
    }
  }

  object CypherFloat extends UnapplyValue[Double, CypherFloat]

  object CypherLocalDateTime extends UnapplyValue[java.time.LocalDateTime, CypherLocalDateTime]

  object CypherDate extends UnapplyValue[java.time.LocalDate, CypherDate]

  object CypherDuration extends UnapplyValue[Duration, CypherDuration]
}
