/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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

import java.lang

import cats.Show
import org.opencypher.caps.api.exception.{CypherException, CypherValueException}
import org.opencypher.caps.api.types.CypherType.OrderGroups._
import org.opencypher.caps.api.types.CypherType._
import org.opencypher.caps.api.types.{CypherType, _}
import org.opencypher.caps.api.value.instances.{cypherList, cypherMap, _}
import org.opencypher.caps.api.value.syntax.cypherNull

import scala.collection.JavaConverters._
import scala.collection.TraversableOnce

object CypherValueCompanion {
  def apply[V <: CypherValue](implicit companion: CypherValueCompanion[V]): CypherValueCompanion[V] = companion
}

sealed trait CypherValueCompanion[V <: CypherValue] extends Equiv[V] with Show[V] {

  type Contents <: Any
  type Input <: Any

  // All CypherValue companions provide various construction and deconstruction facilities
  //
  // create         - construct using canonical scala representation but cannot construct cypherNull
  // from/contents  - (de)construct using canonical scala representation that uses Options to represent cypherNull
  //
  // apply          - like create but supports null and other, non-canonical representations (like java types)
  // unapply        - same as contents
  //

  def apply(v: Input): V
  final def unapply(v: V): Option[Contents] = contents(v)

  def create(contents: Contents): V

  def cypherType(v: V): CypherType

  /**
    * @return Construct an instance from the given scala value representation
    */
  final def from(contents: Option[Contents]): V =
    contents.map(create).getOrElse(cypherNull[V])

  // Scala value representation of this cypher value
  def contents(v: V): Option[Contents]

  final def show(v: V): String = if (isNull(v)) "null" else v.toString

  final def isNull(v: V): Boolean = v == null

  /**
    * @return true if this value is null or contains a null from the viewpoint of what's considered by comparability
    */
  def isOrContainsNull(v: V): Boolean

  /**
    * @return Values in the same order group are ordered (sorted) together by orderability
    */
  def orderGroup(v: V): OrderGroup

  /**
    * @return true if the given values are equivalent (under "Cypher equivalence")
    */
  def equiv(l: V, r: V): Boolean =
    order(l, r) == 0

  /**
    * @return true if the given values are equal (under "Cypher equality")
    */
  def equal(l: V, r: V): Ternary = {
    if (l eq r) {
      if (isOrContainsNull(l)) Maybe else True
    } else {
      if (isOrContainsNull(l) || isOrContainsNull(r))
        Maybe
      else {
        val xGroup = orderGroup(l)
        val yGroup = orderGroup(r)
        if (xGroup == yGroup)
          Ternary(computeOrder(l, r) == 0)
        else
          False
      }
    }
  }

  final val reverseOrder = order.reverse

  /**
    * Cypher orderability
    */
  implicit object order extends Ordering[V] with ((V, V) => Int) {
    override def apply(x: V, y: V): Int = compare(x, y)
    override def compare(x: V, y: V): Int = {
      if (x eq y) 0
      else if (y eq null) -1
      else if (null eq x) +1
      else computeOrder(x, y)
    }
  }

  /**
    * @return the result of comparing the given values (under "Cypher comparability")
    */
  def compare(l: V, r: V): Option[Int] = {
    // This is not a partial order (it is not reflexive!)
    if (l eq r) {
      if (isOrContainsNull(l)) None else Some(0)
    } else {
      if (isOrContainsNull(l) || isOrContainsNull(r))
        None
      else {
        val xGroup = orderGroup(l)
        val yGroup = orderGroup(r)
        if (xGroup == yGroup)
          Some(computeOrder(l, r))
        else
          None
      }
    }
  }

  // Compute orderability of non-null values from possibly different order groups and possibly containing nested nulls
  protected[value] def computeOrder(l: V, r: V): Int

  //TODO: try to remove
  // Compute comparability of values from same order group that are never null or contain nested nulls
  protected[value] def computeCompare(l: V, r: V): Int = computeOrder(l, r)
}

sealed trait CypherScalarValueCompanion[V <: CypherValue] extends CypherValueCompanion[V] {

  override type Input = Contents

  override def isOrContainsNull(v: V): Boolean = isNull(v)

  def apply(value: Input): V = value match {
    case null => cypherNull
    case v    => create(v)
  }
}

// *** ANY

case object CypherValue extends CypherValueCompanion[CypherValue] {

  override type Contents = Any
  override type Input = Any

  override def apply(value: Input): CypherValue = value match {
    case null                                => cypherNull
    case v: CypherValue                      => v
    case v: java.util.List[_] if v.isEmpty   => CypherList.empty
    case v: java.util.List[_]                => cypherList(v.asScala)(CypherValue.apply)
    case v: Array[_] if v.isEmpty            => CypherList.empty
    case v: Array[_]                         => cypherList(v)(CypherValue.apply)
    case ((k: String, v))                    => cypherMap(Properties(k -> CypherValue.apply(v)))
    case v: Map[_, _] if v.isEmpty           => CypherMap.empty
    case v: Map[_, _]                        => cypherMap(v)(CypherValue.apply)
    case v: java.util.Map[_, _] if v.isEmpty => CypherMap.empty
    case v: java.util.Map[_, _]              => cypherMap(v.asScala.toMap)(CypherValue.apply)
    case v: Properties if v.isEmpty          => CypherMap.empty
    case v: Properties                       => cypherMap(v)
    case v                                   => create(v)
  }

  def create(value: Contents): CypherValue = value match {
    case v: String               => cypherString(v)
    case v: java.lang.Byte       => cypherInteger(v)
    case v: java.lang.Short      => cypherInteger(v)
    case v: java.lang.Integer    => cypherInteger(v)
    case v: java.lang.Long       => cypherInteger(v)
    case v: java.lang.Float      => cypherFloat(v)
    case v: java.lang.Double     => cypherFloat(v)
    case v: java.lang.Boolean    => cypherBoolean(v)
    case v: Seq[_] if v.isEmpty  => CypherList.empty
    case v: Seq[_]               => cypherList(v)(CypherValue.apply)
    case v: NodeContents         => cypherNode(v)
    case v: RelationshipContents => cypherRelationship(v)
    case v: RegularMap           => cypherMap(v)
    case _                       => throw CypherValueException(s"Object $value could not be converted to a CypherValue")
  }

  override def cypherType(value: CypherValue): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case v: CypherBoolean => CypherBoolean.cypherType(v)
        case v: CypherString  => CypherString.cypherType(v)
        case v: CypherNumber  => CypherNumber.cypherType(v)
        case v: CypherList    => CypherList.cypherType(v)
        case v: CypherMap     => CypherMap.cypherType(v)
        case v: CypherPath    => CypherPath.cypherType(v)
      }

  override def contents(value: CypherValue): Option[Any] =
    if (isNull(value)) None
    else
      value match {
        case v: CypherBoolean => CypherBoolean.contents(v)
        case v: CypherString  => CypherString.contents(v)
        case v: CypherNumber  => CypherNumber.contents(v)
        case v: CypherList    => CypherList.contents(v)
        case v: CypherMap     => CypherMap.contents(v)
        case v: CypherPath    => CypherPath.contents(v)
      }

  override def orderGroup(value: CypherValue): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case v: CypherBoolean => CypherBoolean.orderGroup(v)
        case v: CypherString  => CypherString.orderGroup(v)
        case v: CypherNumber  => CypherNumber.orderGroup(v)
        case v: CypherList    => CypherList.orderGroup(v)
        case v: CypherMap     => CypherMap.orderGroup(v)
        case v: CypherPath    => CypherPath.orderGroup(v)
      }

  protected[value] def computeOrder(l: CypherValue, r: CypherValue): Int = {
    val lGroup = orderGroup(l)
    val rGroup = orderGroup(r)
    val cmp = lGroup.id - rGroup.id
    if (cmp == 0)
      (l, r) match {
        case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.computeOrder(a, b)
        case (a: CypherString, b: CypherString)   => CypherString.computeOrder(a, b)
        case (a: CypherNumber, b: CypherNumber)   => CypherNumber.computeOrder(a, b)
        case (a: CypherList, b: CypherList)       => CypherList.computeOrder(a, b)
        case (a: CypherMap, b: CypherMap)         => CypherMap.computeOrder(a, b)
        case (a: CypherPath, b: CypherPath)       => CypherPath.computeOrder(a, b)
        case _                                    => throw CypherValueException(s"Order between `$l` and `$r` is undefined")
      } else
      cmp
  }

  //TODO: Try to remove
  protected[value] override def computeCompare(l: CypherValue, r: CypherValue): Int = (l, r) match {
    case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.computeCompare(a, b)
    case (a: CypherString, b: CypherString)   => CypherString.computeCompare(a, b)
    case (a: CypherNumber, b: CypherNumber)   => CypherNumber.computeCompare(a, b)
    case (a: CypherList, b: CypherList)       => CypherList.computeCompare(a, b)
    case (a: CypherMap, b: CypherMap)         => CypherMap.computeCompare(a, b)
    case (a: CypherPath, b: CypherPath)       => CypherPath.computeCompare(a, b)
    case _ =>
      throw CypherValueException(s"Comparison between `$l` and `$r` is undefined")
  }

  override def isOrContainsNull(value: CypherValue): Boolean =
    if (isNull(value)) true
    else
      value match {
        case v: CypherBoolean => CypherBoolean.isOrContainsNull(v)
        case v: CypherString  => CypherString.isOrContainsNull(v)
        case v: CypherNumber  => CypherNumber.isOrContainsNull(v)
        case v: CypherList    => CypherList.isOrContainsNull(v)
        case v: CypherMap     => CypherMap.isOrContainsNull(v)
        case v: CypherPath    => CypherPath.isOrContainsNull(v)
      }
}

sealed trait CypherValue {
  self: Serializable =>
}

// *** BOOLEAN

case object CypherBoolean extends CypherScalarValueCompanion[CypherBoolean] {

  override type Contents = Boolean

  val TRUE = new CypherBoolean(true)
  val FALSE = new CypherBoolean(false)

  override def create(value: Contents): CypherBoolean =
    if (value) TRUE else FALSE

  override def cypherType(value: CypherBoolean): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTBoolean

  override def contents(value: CypherBoolean): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  def orderGroup(value: CypherBoolean): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else BooleanOrderGroup

  override protected[value] def computeOrder(l: CypherBoolean, r: CypherBoolean): Int =
    Ordering.Boolean.compare(l.v, r.v)
}

final class CypherBoolean(private[CypherBoolean] val v: Boolean) extends CypherValue with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherBoolean => CypherBoolean.equiv(this, other)
    case _                    => false
  }

  override def toString: String = if (v) "true" else "false"
}

// *** STRING

case object CypherString extends CypherScalarValueCompanion[CypherString] {

  override type Contents = String

  def create(value: String): CypherString =
    new CypherString(value)

  override def cypherType(value: CypherString): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTString

  override def contents(value: CypherString): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  def orderGroup(value: CypherString): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else StringOrderGroup

  override protected[value] def computeOrder(l: CypherString, r: CypherString): Int =
    Ordering.String.compare(l.v, r.v)
}

final class CypherString(private[CypherString] val v: String) extends CypherValue with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherString => CypherString.equiv(this, other)
    case _                   => false
  }

  override def toString: String = s"'${v.replaceAllLiterally("'", "\'")}'"
}

// *** NUMBER

sealed trait CypherNumberCompanion[V <: CypherNumber] extends CypherScalarValueCompanion[V] {

  override type Contents <: Number

  final def orderGroup(value: V): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else NumberOrderGroup
}

case object CypherNumber extends CypherNumberCompanion[CypherNumber] {

  override type Contents = Number

  def create(contents: Contents): CypherNumber = contents match {
    case _: lang.Long | _: lang.Integer | _: lang.Short | _: lang.Byte => CypherInteger(contents.longValue)
    case _: lang.Double | _: lang.Float                                => CypherFloat(contents.doubleValue)
    case _ =>
      throw CypherValueException(s"$contents is not a CypherNumber")
  }

  override def cypherType(value: CypherNumber): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case v: CypherInteger => CypherInteger.cypherType(v)
        case v: CypherFloat   => CypherFloat.cypherType(v)
      }

  override def contents(value: CypherNumber): Option[Contents] =
    if (isNull(value)) None
    else
      value match {
        case CypherInteger(v) => Some(v)
        case CypherFloat(v)   => Some(v)
      }

  override protected[value] def computeOrder(l: CypherNumber, r: CypherNumber): Int = (l, r) match {
    case (a: CypherInteger, b: CypherInteger) =>
      CypherInteger.computeOrder(a, b)

    case (a: CypherFloat, b: CypherFloat) =>
      CypherFloat.computeOrder(a, b)

    case (a @ CypherFloat(f), b @ CypherInteger(i)) =>
      if (fitsDouble(i)) CypherFloat.computeOrder(a, CypherFloat(i.toDouble))
      else BigDecimal.decimal(f).compare(BigDecimal.decimal(i))

    case (a @ CypherInteger(i), b @ CypherFloat(f)) =>
      if (fitsDouble(i)) CypherFloat.computeOrder(CypherFloat(i.toDouble), b)
      else BigDecimal.decimal(i).compare(BigDecimal.decimal(f))
  }

  // TODO: Get code for this
  private def fitsDouble(v: Long): Boolean = true
}

sealed trait CypherNumber extends CypherValue {
  self: Serializable =>
}

// *** INTEGER

case object CypherInteger extends CypherNumberCompanion[CypherInteger] {

  override type Contents = lang.Long

  def create(value: Contents): CypherInteger = new CypherInteger(value)

  override def cypherType(value: CypherInteger): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTInteger

  override def contents(value: CypherInteger): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  override protected[value] def computeOrder(l: CypherInteger, r: CypherInteger): Int =
    Ordering.Long.compare(l.v, r.v)
}

final class CypherInteger(private[CypherInteger] val v: Long) extends CypherNumber with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherInteger => CypherInteger.equiv(this, other)
    case other: CypherNumber  => CypherNumber.equiv(this, other)
    case _                    => false
  }

  override def toString: String = s"$v"
}

// *** FLOAT ***

case object CypherFloat extends CypherNumberCompanion[CypherFloat] {

  override type Contents = lang.Double

  def create(value: Contents): CypherFloat =
    new CypherFloat(value)

  override def cypherType(value: CypherFloat): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTFloat

  override def contents(value: CypherFloat): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  override protected[value] def computeOrder(l: CypherFloat, r: CypherFloat): Int = {
    val lVal = l.v
    val rVal = r.v
    if (lVal.isNaN) {
      if (rVal.isNaN) 0 else +1
    } else {
      if (rVal.isNaN) -1 else Ordering.Double.compare(lVal, rVal)
    }
  }
}

final class CypherFloat(private[CypherFloat] val v: Double) extends CypherNumber with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherFloat  => CypherFloat.equiv(this, other)
    case other: CypherNumber => CypherNumber.equiv(this, other)
    case _                   => false
  }

  override def toString: String = s"$v"
}

// *** LIST

case object CypherList extends CypherValueCompanion[CypherList] {

  override type Contents = Seq[CypherValue]
  override type Input = Any

  object empty extends CypherList(Seq.empty)

  override def apply(value: Input): CypherList = value match {
    case null                              => cypherNull
    case v: CypherList                     => v
    case v: Seq[_] if v.isEmpty            => CypherList.empty
    case v: Seq[_]                         => cypherList(v)(CypherValue.apply)
    case v: java.util.List[_] if v.isEmpty => CypherList.empty
    case v: java.util.List[_]              => cypherList(v.asScala)(CypherValue.apply)
    case v: Array[_] if v.isEmpty          => CypherList.empty
    case v: Array[_]                       => cypherList(v)(CypherValue.apply)
    case v                                 => throw CypherValueException(s"$v is not a CypherList")
  }

  def create(value: Contents): CypherList =
    if (value.isEmpty) CypherList.empty else new CypherList(value)

  override def cypherType(value: CypherList): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTList(value.cachedElementType)

  override def contents(value: CypherList): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  override def isOrContainsNull(value: CypherList): Boolean =
    isNull(value) || value.cachedIsOrContainsNulls

  override protected[value] def computeOrder(l: CypherList, r: CypherList): Int =
    valueListOrderability.compare(l.v, r.v)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherList): OrderGroup =
    if (isNull(value)) VoidOrderGroup else ListOrderGroup

  private val valueListOrderability = Ordering.Iterable(CypherValue.order)
}

sealed class CypherList(private[CypherList] val v: Seq[CypherValue]) extends CypherValue with Serializable {

  def map[B](f: CypherValue => B): TraversableOnce[B] = v.map(f)

  @transient
  private[CypherList] lazy val cachedIsOrContainsNulls: Boolean =
    v.exists(CypherValue.isOrContainsNull)

  @transient
  private[CypherList] lazy val cachedElementType: CypherType =
    v.map(CypherValue.cypherType).reduceOption(_ join _).getOrElse(CTVoid)

  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherList => CypherList.equiv(this, other)
    case _                 => false
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append('[')
    var first = true
    val iter = v.iterator
    while (iter.hasNext) {
      if (first)
        first = false
      else {
        builder.append(',')
        builder.append(' ')
      }
      builder.append(iter.next())
    }

    builder.append(']')
    builder.result()
  }
}

// *** MAP

sealed trait CypherMapCompanion[V <: CypherMap] extends CypherValueCompanion[V] {

  override type Contents <: MapContents

  def property(value: V)(name: String): CypherValue =
    if (isNull(value)) null else value.properties(name)

  def properties(value: V): Option[Properties] =
    if (isNull(value)) None else Some(value.properties)
}

case object CypherMap extends CypherMapCompanion[CypherMap] {

  override type Contents = MapContents
  override type Input = Any

  object empty extends CypherMap(Properties.empty)

  override def apply(value: Input): CypherMap = value match {
    case null                                => cypherNull
    case v: CypherMap                        => v
    case ((k: String, v))                    => cypherMap(Properties(k -> CypherValue.apply(v)))
    case v: Map[_, _] if v.isEmpty           => CypherMap.empty
    case v: Map[_, _]                        => cypherMap(v)(CypherValue.apply)
    case v: java.util.Map[_, _] if v.isEmpty => CypherMap.empty
    case v: java.util.Map[_, _]              => cypherMap(v.asScala.toMap)(CypherValue.apply)
    case v: MapContents                      => cypherMap(v)
    case v: Properties if v.isEmpty          => CypherMap.empty
    case v: Properties                       => cypherMap(v)
    case v                                   => throw CypherValueException(s"$v is not a CypherMap")
  }

  def apply(elts: (String, CypherValue)*): CypherMap =
    create(if (elts.isEmpty) Properties.empty else Properties(elts: _*))

  def create(value: Contents): CypherMap = value match {
    case m: RegularMap             => create(m.properties)
    case node: NodeContents        => CypherNode.create(node)
    case rel: RelationshipContents => CypherRelationship.create(rel)
  }

  def create(value: Properties): CypherMap =
    if (value == Properties.empty) empty else new CypherMap(value)

  override def isOrContainsNull(v: CypherMap): Boolean =
    isNull(v) || (v match {
      case entity: CypherEntityValue => CypherEntityCompanion.isOrContainsNull(entity)
      case _                         => v.cachedIsOrContainsNulls
    })

  override def cypherType(value: CypherMap): CypherType =
    if (value == null) CTNull
    else
      value match {
        case entity: CypherEntityValue => CypherEntityCompanion.cypherType(entity)
        case _                         => CTMap
      }

  override def contents(value: CypherMap): Option[Contents] =
    if (value == null) None
    else
      value match {
        case entity: CypherEntityValue => CypherEntityCompanion.contents(entity)
        case _                         => properties(value).map(RegularMap)
      }

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherMap): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case entity: CypherEntityValue => CypherEntityCompanion.orderGroup(entity)
        case _                         => MapOrderGroup
      }

  protected[value] def computeOrder(l: CypherMap, r: CypherMap): Int = (l, r) match {
    case (a: CypherEntityValue, b: CypherEntityValue) => CypherEntityCompanion.computeOrder(a, b)
    case (a: CypherMap, b: CypherMap)                 => mapEntryOrdering.compare(l.properties.m, r.properties.m)
  }

  private val mapEntryOrdering =
    Ordering.Iterable(Ordering.Tuple2(Ordering.String, CypherValue.order))
}

sealed class CypherMap(protected[value] val properties: Properties) extends CypherValue with Serializable {

  def get(key: String): Option[CypherValue] = properties.get(key)

  def keys: Set[String] = properties.m.keySet

  def values: Iterable[CypherValue] = properties.m.values

  @transient
  protected[value] lazy val cachedIsOrContainsNulls: Boolean =
    properties.m.values.exists(CypherValue.isOrContainsNull)

  override def hashCode(): Int = properties.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherEntityValue => false
    case other: CypherMap         => CypherMap.equiv(this, other)
    case _                        => false
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append('{')
    var first = true
    val iter = properties.m.iterator
    while (iter.hasNext) {
      if (first)
        first = false
      else {
        builder.append(',')
        builder.append(' ')
      }
      val (k, v) = iter.next
      builder.append(k)
      builder.append(':')
      builder.append(' ')
      builder.append(v)
    }

    builder.append('}')
    builder.result()
  }
}

// *** ENTITY

case object CypherEntityCompanion extends CypherEntityCompanion[CypherEntityValue] {

  override type Contents = EntityContents

  def create(contents: EntityContents): CypherEntityValue =
    contents match {
      case node: NodeContents        => CypherNode.create(node)
      case rel: RelationshipContents => CypherRelationship.create(rel)
    }

  override def contents(value: CypherEntityValue): Option[Contents] = {
    if (isNull(value)) None
    else
      value match {
        case node: CypherNode        => CypherNode.contents(node)
        case rel: CypherRelationship => CypherRelationship.contents(rel)
      }
  }

  override def cypherType(value: CypherEntityValue): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case node: CypherNode        => CypherNode.cypherType(node)
        case rel: CypherRelationship => CypherRelationship.cypherType(rel)
      }

  override def orderGroup(value: CypherEntityValue): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case node: CypherNode        => CypherNode.orderGroup(node)
        case rel: CypherRelationship => CypherRelationship.orderGroup(rel)
      }
}

sealed trait CypherEntityCompanion[V <: CypherEntityValue] extends CypherMapCompanion[V] {

  override type Contents <: EntityContents
  override type Input = Contents

  def apply(value: Input): V = value match {
    case null => cypherNull
    case v    => create(v)
  }

  // Entities are compared by id, therefore nulls in properties are not considered
  override def isOrContainsNull(v: V): Boolean = isNull(v)

  def id(v: V): Option[EntityId] = if (isNull(v)) None else Some(v.id)

  override protected[value] def computeOrder(l: V, r: V): Int =
    EntityId.ordering.compare(l.id, r.id)
}

sealed abstract class CypherEntityValue(override protected[value] val properties: Properties)
    extends CypherMap(properties) {

  protected[value] def id: EntityId
  protected[value] def data: EntityData
}

// *** NODE

case object CypherNode extends CypherEntityCompanion[CypherNode] {

  override type Contents = NodeContents

  def apply(id: EntityId, data: NodeData): CypherNode =
    apply(id, data.labels, data.properties)

  def apply(id: EntityId, labels: Seq[String], properties: Properties): CypherNode =
    new CypherNode(id, labels, properties)

  def create(contents: NodeContents): CypherNode =
    apply(contents.id, contents.labels, contents.properties)

  override def contents(value: CypherNode): Option[Contents] =
    if (value == null) None else Some(NodeContents(value.id, value.labels, value.properties))

  override def cypherType(value: CypherNode): CypherType with DefiniteCypherType =
    if (value == null) CTNull else CTNode(labels(value).toSeq.flatten: _*)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherNode): OrderGroup =
    if (isNull(value)) VoidOrderGroup else NodeOrderGroup

  def labels(node: CypherNode): Option[Seq[String]] =
    if (isNull(node)) None else Some(node.labels)
}

sealed class CypherNode(
    protected[value] val id: EntityId,
    // TODO: Use Set once available in Spark 2.3
    protected[value] val labels: Seq[String],
    override protected[value] val properties: Properties)
    extends CypherEntityValue(properties)
    with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherNode => CypherNode.equiv(this, other)
    case _                 => false
  }

  override protected[value] def data = NodeData(labels, properties.m)

  override def toString = {
    val lbls = if (labels.isEmpty) "" else labels.mkString(":", ":", "")
    val props = if (properties.isEmpty) "" else super.toString
    Seq(lbls, props).filter(_.nonEmpty).mkString("(", " ", ")")
  }
}

// *** RELATIONSHIP

case object CypherRelationship extends CypherEntityCompanion[CypherRelationship] {

  override type Contents = RelationshipContents

  def apply(id: EntityId, data: RelationshipData): CypherRelationship =
    new CypherRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)

  def apply(
      id: EntityId,
      startId: EntityId,
      endId: EntityId,
      relType: String,
      properties: Properties): CypherRelationship =
    new CypherRelationship(id, startId, endId, relType, properties)

  def create(contents: RelationshipContents): CypherRelationship =
    apply(contents.id, contents.startId, contents.endId, contents.relationshipType, contents.properties)

  override def contents(value: CypherRelationship): Option[RelationshipContents] =
    if (isNull(value))
      None
    else
      Some(RelationshipContents(value.id, value.startId, value.endId, value.relationshipType, value.properties))

  override def cypherType(value: CypherRelationship): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else relationshipType(value).map(t => CTRelationship(t)).getOrElse(CTRelationship)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherRelationship): OrderGroup =
    if (isNull(value)) VoidOrderGroup else RelationshipOrderGroup

  def relationshipType(value: CypherRelationship): Option[String] =
    if (isNull(value)) None else Some(value.relationshipType)

  def startId(value: CypherRelationship): Option[EntityId] =
    if (isNull(value)) None else Some(value.startId)

  def endId(value: CypherRelationship): Option[EntityId] =
    if (isNull(value)) None else Some(value.endId)
}

sealed class CypherRelationship(
    protected[value] val id: EntityId,
    protected[value] val startId: EntityId,
    protected[value] val endId: EntityId,
    protected[value] val relationshipType: String,
    override protected[value] val properties: Properties)
    extends CypherEntityValue(properties)
    with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherRelationship => CypherRelationship.equiv(this, other)
    case _                         => false
  }

  override protected[value] def data = RelationshipData(startId, endId, relationshipType, properties.m)

  override def toString = {
    val props = if (properties.isEmpty) "" else s" ${super.toString}"
    s"[:$relationshipType$props]"
  }
}

// *** Path

case object CypherPath extends CypherValueCompanion[CypherPath] {

  override type Contents = Seq[CypherEntityValue]
  override type Input = Contents

  def apply(value: Input): CypherPath = value match {
    case null => cypherNull
    case v    => create(v)
  }

  def create(contents: Seq[CypherEntityValue]): CypherPath =
    // TODO: Prevent construction of invalid paths, cf CypherTestValue suite for some initial code for this
    new CypherPath(contents)

  override def cypherType(value: CypherPath): CypherType =
    if (isNull(value)) CTNull else CTPath

  override def contents(value: CypherPath): Option[Contents] =
    if (isNull(value)) None else Some(value.elements)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherPath): OrderGroup =
    if (isNull(value)) VoidOrderGroup else PathOrderGroup

  override protected[value] def computeOrder(l: CypherPath, r: CypherPath): Int =
    pathOrdering.compare(l.elements, r.elements)

  private val pathOrdering = Ordering.Iterable(CypherEntityCompanion.order)

  // A path containing null entities is malformed and therefore not considered here
  override def isOrContainsNull(v: CypherPath): Boolean = isNull(v)
}

sealed class CypherPath(protected[value] val elements: Seq[CypherEntityValue]) extends CypherValue with Serializable {

  override def hashCode(): Int = elements.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherPath => CypherPath.equiv(this, other)
    case _                 => false
  }

  override def toString: String = s"<${elements.mkString(",")}>"
}
