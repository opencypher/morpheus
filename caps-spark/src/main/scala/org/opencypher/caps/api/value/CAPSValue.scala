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

import java.lang

import cats.Show
import org.opencypher.caps.api.exception.CypherValueException
import org.opencypher.caps.api.graph._
import org.opencypher.caps.api.types.CypherType.OrderGroups._
import org.opencypher.caps.api.types.CypherType._
import org.opencypher.caps.api.types.{CypherType, _}
import org.opencypher.caps.api.value.instances.{cypherList, cypherMap, _}
import org.opencypher.caps.api.value.syntax.cypherNull

import scala.collection.JavaConverters._
import scala.collection.TraversableOnce

object CAPSValueCompanion {
  def apply[V <: CAPSValue](implicit companion: CAPSValueCompanion[V]): CAPSValueCompanion[V] = companion
}

sealed trait CAPSValueCompanion[V <: CAPSValue] extends Equiv[V] with Show[V] {

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

sealed trait CAPSScalarValueCompanion[V <: CAPSValue] extends CAPSValueCompanion[V] {

  override type Input = Contents

  override def isOrContainsNull(v: V): Boolean = isNull(v)

  def apply(value: Input): V = value match {
    case null => cypherNull
    case v => create(v)
  }
}

// *** ANY

case object CAPSValue extends CAPSValueCompanion[CAPSValue] {

  override type Contents = Any
  override type Input = Any

  override def apply(value: Input): CAPSValue = value match {
    case null => cypherNull
    case v: CAPSValue => v
    case v: java.util.List[_] if v.isEmpty => CAPSList.empty
    case v: java.util.List[_] => cypherList(v.asScala)(CAPSValue.apply)
    case v: Array[_] if v.isEmpty => CAPSList.empty
    case v: Array[_] => cypherList(v)(CAPSValue.apply)
    case ((k: String, v)) => cypherMap(Properties(k -> CAPSValue.apply(v)))
    case v: Map[_, _] if v.isEmpty => CAPSMap.empty
    case v: Map[_, _] => cypherMap(v)(CAPSValue.apply)
    case v: java.util.Map[_, _] if v.isEmpty => CAPSMap.empty
    case v: java.util.Map[_, _] => cypherMap(v.asScala.toMap)(CAPSValue.apply)
    case v: Properties if v.isEmpty => CAPSMap.empty
    case v: Properties => cypherMap(v)
    case v => create(v)
  }

  def create(value: Contents): CAPSValue = value match {
    case v: String => cypherString(v)
    case v: java.lang.Byte => cypherInteger(v)
    case v: java.lang.Short => cypherInteger(v)
    case v: java.lang.Integer => cypherInteger(v)
    case v: java.lang.Long => cypherInteger(v)
    case v: java.lang.Float => cypherFloat(v)
    case v: java.lang.Double => cypherFloat(v)
    case v: java.lang.Boolean => cypherBoolean(v)
    case v: Seq[_] if v.isEmpty => CAPSList.empty
    case v: Seq[_] => cypherList(v)(CAPSValue.apply)
    case v: NodeContents => cypherNode(v)
    case v: RelationshipContents => cypherRelationship(v)
    case v: RegularMap => cypherMap(v)
    case _ => throw CypherValueException(s"Object $value could not be converted to a CypherValue")
  }

  override def cypherType(value: CAPSValue): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case v: CAPSBoolean => CAPSBoolean.cypherType(v)
        case v: CAPSString => CAPSString.cypherType(v)
        case v: CAPSNumber => CAPSNumber.cypherType(v)
        case v: CAPSList => CAPSList.cypherType(v)
        case v: CAPSMap => CAPSMap.cypherType(v)
        case v: CAPSPath => CAPSPath.cypherType(v)
      }

  override def contents(value: CAPSValue): Option[Any] =
    if (isNull(value)) None
    else
      value match {
        case v: CAPSBoolean => CAPSBoolean.contents(v)
        case v: CAPSString => CAPSString.contents(v)
        case v: CAPSNumber => CAPSNumber.contents(v)
        case v: CAPSList => CAPSList.contents(v)
        case v: CAPSMap => CAPSMap.contents(v)
        case v: CAPSPath => CAPSPath.contents(v)
      }

  override def orderGroup(value: CAPSValue): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case v: CAPSBoolean => CAPSBoolean.orderGroup(v)
        case v: CAPSString => CAPSString.orderGroup(v)
        case v: CAPSNumber => CAPSNumber.orderGroup(v)
        case v: CAPSList => CAPSList.orderGroup(v)
        case v: CAPSMap => CAPSMap.orderGroup(v)
        case v: CAPSPath => CAPSPath.orderGroup(v)
      }

  protected[value] def computeOrder(l: CAPSValue, r: CAPSValue): Int = {
    val lGroup = orderGroup(l)
    val rGroup = orderGroup(r)
    val cmp = lGroup.id - rGroup.id
    if (cmp == 0)
      (l, r) match {
        case (a: CAPSBoolean, b: CAPSBoolean) => CAPSBoolean.computeOrder(a, b)
        case (a: CAPSString, b: CAPSString) => CAPSString.computeOrder(a, b)
        case (a: CAPSNumber, b: CAPSNumber) => CAPSNumber.computeOrder(a, b)
        case (a: CAPSList, b: CAPSList) => CAPSList.computeOrder(a, b)
        case (a: CAPSMap, b: CAPSMap) => CAPSMap.computeOrder(a, b)
        case (a: CAPSPath, b: CAPSPath) => CAPSPath.computeOrder(a, b)
        case _ => throw CypherValueException(s"Order between `$l` and `$r` is undefined")
      } else
      cmp
  }

  //TODO: Try to remove
  protected[value] override def computeCompare(l: CAPSValue, r: CAPSValue): Int = (l, r) match {
    case (a: CAPSBoolean, b: CAPSBoolean) => CAPSBoolean.computeCompare(a, b)
    case (a: CAPSString, b: CAPSString) => CAPSString.computeCompare(a, b)
    case (a: CAPSNumber, b: CAPSNumber) => CAPSNumber.computeCompare(a, b)
    case (a: CAPSList, b: CAPSList) => CAPSList.computeCompare(a, b)
    case (a: CAPSMap, b: CAPSMap) => CAPSMap.computeCompare(a, b)
    case (a: CAPSPath, b: CAPSPath) => CAPSPath.computeCompare(a, b)
    case _ =>
      throw CypherValueException(s"Comparison between `$l` and `$r` is undefined")
  }

  override def isOrContainsNull(value: CAPSValue): Boolean =
    if (isNull(value)) true
    else
      value match {
        case v: CAPSBoolean => CAPSBoolean.isOrContainsNull(v)
        case v: CAPSString => CAPSString.isOrContainsNull(v)
        case v: CAPSNumber => CAPSNumber.isOrContainsNull(v)
        case v: CAPSList => CAPSList.isOrContainsNull(v)
        case v: CAPSMap => CAPSMap.isOrContainsNull(v)
        case v: CAPSPath => CAPSPath.isOrContainsNull(v)
      }
}

sealed trait CAPSValue extends CypherValue {
  self: Serializable =>
}

// *** BOOLEAN

case object CAPSBoolean extends CAPSScalarValueCompanion[CAPSBoolean] {

  override type Contents = Boolean

  val TRUE = new CAPSBoolean(true)
  val FALSE = new CAPSBoolean(false)

  override def create(value: Contents): CAPSBoolean =
    if (value) TRUE else FALSE

  override def cypherType(value: CAPSBoolean): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTBoolean

  override def contents(value: CAPSBoolean): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  def orderGroup(value: CAPSBoolean): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else BooleanOrderGroup

  override protected[value] def computeOrder(l: CAPSBoolean, r: CAPSBoolean): Int =
    Ordering.Boolean.compare(l.v, r.v)
}

final class CAPSBoolean(private[CAPSBoolean] val v: Boolean) extends CAPSValue with CypherBoolean with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSBoolean => CAPSBoolean.equiv(this, other)
    case _ => false
  }

  override def toString: String = if (v) "true" else "false"
}

// *** STRING

case object CAPSString extends CAPSScalarValueCompanion[CAPSString] {

  override type Contents = String

  def create(value: String): CAPSString =
    new CAPSString(value)

  override def cypherType(value: CAPSString): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTString

  override def contents(value: CAPSString): Option[Contents] =
    if (isNull(value)) None else Some(value.value)

  def orderGroup(value: CAPSString): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else StringOrderGroup

  override protected[value] def computeOrder(l: CAPSString, r: CAPSString): Int =
    Ordering.String.compare(l.value, r.value)
}

final class CAPSString(val value: String) extends CAPSValue with CypherString with Serializable {
  override def hashCode(): Int = value.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSString => CAPSString.equiv(this, other)
    case _ => false
  }

  override def toString: String = s"'${value.replaceAllLiterally("'", "\'")}'"
}

// *** NUMBER

sealed trait CAPSNumberCompanion[V <: CAPSNumber] extends CAPSScalarValueCompanion[V] {

  override type Contents <: Number

  final def orderGroup(value: V): CypherType.OrderGroups.Value =
    if (isNull(value)) VoidOrderGroup else NumberOrderGroup
}

case object CAPSNumber extends CAPSNumberCompanion[CAPSNumber] {

  override type Contents = Number

  def create(contents: Contents): CAPSNumber = contents match {
    case _: lang.Long | _: lang.Integer | _: lang.Short | _: lang.Byte => CAPSInteger(contents.longValue)
    case _: lang.Double | _: lang.Float => CAPSFloat(contents.doubleValue)
    case _ =>
      throw CypherValueException(s"$contents is not a CypherNumber")
  }

  override def cypherType(value: CAPSNumber): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case v: CAPSInteger => CAPSInteger.cypherType(v)
        case v: CAPSFloat => CAPSFloat.cypherType(v)
      }

  override def contents(value: CAPSNumber): Option[Contents] =
    if (isNull(value)) None
    else
      value match {
        case CAPSInteger(v) => Some(v)
        case CAPSFloat(v) => Some(v)
      }

  override protected[value] def computeOrder(l: CAPSNumber, r: CAPSNumber): Int = (l, r) match {
    case (a: CAPSInteger, b: CAPSInteger) =>
      CAPSInteger.computeOrder(a, b)

    case (a: CAPSFloat, b: CAPSFloat) =>
      CAPSFloat.computeOrder(a, b)

    case (a@CAPSFloat(f), b@CAPSInteger(i)) =>
      if (fitsDouble(i)) CAPSFloat.computeOrder(a, CAPSFloat(i.toDouble))
      else BigDecimal.decimal(f).compare(BigDecimal.decimal(i))

    case (a@CAPSInteger(i), b@CAPSFloat(f)) =>
      if (fitsDouble(i)) CAPSFloat.computeOrder(CAPSFloat(i.toDouble), b)
      else BigDecimal.decimal(i).compare(BigDecimal.decimal(f))
  }

  // TODO: Get code for this
  private def fitsDouble(v: Long): Boolean = true
}

sealed trait CAPSNumber extends CAPSValue {
  self: Serializable =>
}

// *** INTEGER

case object CAPSInteger extends CAPSNumberCompanion[CAPSInteger] {

  override type Contents = lang.Long

  def create(value: Contents): CAPSInteger = new CAPSInteger(value)

  override def cypherType(value: CAPSInteger): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTInteger

  override def contents(value: CAPSInteger): Option[Contents] =
    if (isNull(value)) None else Some(value.value)

  override protected[value] def computeOrder(l: CAPSInteger, r: CAPSInteger): Int =
    Ordering.Long.compare(l.value, r.value)
}

final class CAPSInteger(val v: Long) extends CAPSNumber with CypherInteger with Serializable {
  override def hashCode(): Int = value.hashCode()

  def value = v

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSInteger => CAPSInteger.equiv(this, other)
    case other: CAPSNumber => CAPSNumber.equiv(this, other)
    case _ => false
  }

  override def toString: String = s"$value"
}

// *** FLOAT ***

case object CAPSFloat extends CAPSNumberCompanion[CAPSFloat] {

  override type Contents = lang.Double

  def create(value: Contents): CAPSFloat =
    new CAPSFloat(value)

  override def cypherType(value: CAPSFloat): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTFloat

  override def contents(value: CAPSFloat): Option[Contents] =
    if (isNull(value)) None else Some(value.value)

  override protected[value] def computeOrder(l: CAPSFloat, r: CAPSFloat): Int = {
    val lVal = l.value
    val rVal = r.value
    if (lVal.isNaN) {
      if (rVal.isNaN) 0 else +1
    } else {
      if (rVal.isNaN) -1 else Ordering.Double.compare(lVal, rVal)
    }
  }
}

final class CAPSFloat(val value: Double) extends CAPSNumber with CypherFloat with Serializable {
  override def hashCode(): Int = value.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSFloat => CAPSFloat.equiv(this, other)
    case other: CAPSNumber => CAPSNumber.equiv(this, other)
    case _ => false
  }

  override def toString: String = s"$value"
}

// *** LIST

case object CAPSList extends CAPSValueCompanion[CAPSList] {

  override type Contents = Seq[CAPSValue]
  override type Input = Any

  object empty extends CAPSList(Seq.empty)

  override def apply(value: Input): CAPSList = value match {
    case null => cypherNull
    case v: CAPSList => v
    case v: Seq[_] if v.isEmpty => CAPSList.empty
    case v: Seq[_] => cypherList(v)(CAPSValue.apply)
    case v: java.util.List[_] if v.isEmpty => CAPSList.empty
    case v: java.util.List[_] => cypherList(v.asScala)(CAPSValue.apply)
    case v: Array[_] if v.isEmpty => CAPSList.empty
    case v: Array[_] => cypherList(v)(CAPSValue.apply)
    case v => throw CypherValueException(s"$v is not a CypherList")
  }

  def create(value: Contents): CAPSList =
    if (value.isEmpty) CAPSList.empty else new CAPSList(value)

  override def cypherType(value: CAPSList): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else CTList(value.cachedElementType)

  override def contents(value: CAPSList): Option[Contents] =
    if (isNull(value)) None else Some(value.v)

  override def isOrContainsNull(value: CAPSList): Boolean =
    isNull(value) || value.cachedIsOrContainsNulls

  override protected[value] def computeOrder(l: CAPSList, r: CAPSList): Int =
    valueListOrderability.compare(l.v, r.v)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CAPSList): OrderGroup =
    if (isNull(value)) VoidOrderGroup else ListOrderGroup

  private val valueListOrderability = Ordering.Iterable(CAPSValue.order)
}

sealed class CAPSList(private[CAPSList] val v: Seq[CAPSValue]) extends CAPSValue with Serializable {

  override def cypherType = CTList(cachedElementType)

  def map[B](f: CAPSValue => B): TraversableOnce[B] = v.map(f)

  @transient
  private[CAPSList] lazy val cachedIsOrContainsNulls: Boolean =
    v.exists(CAPSValue.isOrContainsNull)

  @transient
  private[CAPSList] lazy val cachedElementType: CypherType =
    v.map(CAPSValue.cypherType).reduceOption(_ join _).getOrElse(CTVoid)

  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSList => CAPSList.equiv(this, other)
    case _ => false
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

sealed trait CAPSMapCompanion[V <: CAPSMap] extends CAPSValueCompanion[V] {

  override type Contents <: MapContents

  def property(value: V)(name: String): CAPSValue =
    if (isNull(value)) null else value.properties(name)

  def properties(value: V): Option[Properties] =
    if (isNull(value)) None else Some(value.properties)
}

case object CAPSMap extends CAPSMapCompanion[CAPSMap] {

  override type Contents = MapContents
  override type Input = Any

  object empty extends CAPSMap(Properties.empty)

  override def apply(value: Input): CAPSMap = value match {
    case null => cypherNull
    case v: CAPSMap => v
    case ((k: String, v)) => cypherMap(Properties(k -> CAPSValue.apply(v)))
    case v: Map[_, _] if v.isEmpty => CAPSMap.empty
    case v: Map[_, _] => cypherMap(v)(CAPSValue.apply)
    case v: java.util.Map[_, _] if v.isEmpty => CAPSMap.empty
    case v: java.util.Map[_, _] => cypherMap(v.asScala.toMap)(CAPSValue.apply)
    case v: MapContents => cypherMap(v)
    case v: Properties if v.isEmpty => CAPSMap.empty
    case v: Properties => cypherMap(v)
    case v => throw CypherValueException(s"$v is not a CypherMap")
  }

  def apply(elts: (String, CAPSValue)*): CAPSMap =
    create(if (elts.isEmpty) Properties.empty else Properties(elts: _*))

  def create(value: Contents): CAPSMap = value match {
    case m: RegularMap => create(m.properties)
    case node: NodeContents => CAPSNode.create(node)
    case rel: RelationshipContents => CAPSRelationship.create(rel)
  }

  def create(value: Properties): CAPSMap =
    if (value == Properties.empty) empty else new CAPSMap(value)

  override def isOrContainsNull(v: CAPSMap): Boolean =
    isNull(v) || (v match {
      case entity: CAPSEntityValue => CAPSEntityCompanion.isOrContainsNull(entity)
      case _ => v.cachedIsOrContainsNulls
    })

  override def cypherType(value: CAPSMap): CypherType =
    if (value == null) CTNull
    else
      value match {
        case entity: CAPSEntityValue => CAPSEntityCompanion.cypherType(entity)
        case _ => CTMap
      }

  override def contents(value: CAPSMap): Option[Contents] =
    if (value == null) None
    else
      value match {
        case entity: CAPSEntityValue => CAPSEntityCompanion.contents(entity)
        case _ => properties(value).map(RegularMap)
      }

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CAPSMap): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case entity: CAPSEntityValue => CAPSEntityCompanion.orderGroup(entity)
        case _ => MapOrderGroup
      }

  protected[value] def computeOrder(l: CAPSMap, r: CAPSMap): Int = (l, r) match {
    case (a: CAPSEntityValue, b: CAPSEntityValue) => CAPSEntityCompanion.computeOrder(a, b)
    case (a: CAPSMap, b: CAPSMap) => mapEntryOrdering.compare(l.properties.m, r.properties.m)
  }

  private val mapEntryOrdering =
    Ordering.Iterable(Ordering.Tuple2(Ordering.String, CAPSValue.order))
}

sealed class CAPSMap(protected[value] val properties: Properties) extends CAPSValue with CypherMap with Serializable {

  def get(key: String): Option[CAPSValue] = properties.get(key)

  def keys: Set[String] = properties.m.keySet

  def values: Iterable[CAPSValue] = properties.m.values

  @transient
  protected[value] lazy val cachedIsOrContainsNulls: Boolean =
    properties.m.values.exists(CAPSValue.isOrContainsNull)

  override def hashCode(): Int = properties.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSEntityValue => false
    case other: CAPSMap => CAPSMap.equiv(this, other)
    case _ => false
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

case object CAPSEntityCompanion extends CAPSEntityCompanion[CAPSEntityValue] {

  override type Contents = EntityContents

  def create(contents: EntityContents): CAPSEntityValue =
    contents match {
      case node: NodeContents => CAPSNode.create(node)
      case rel: RelationshipContents => CAPSRelationship.create(rel)
    }

  override def contents(value: CAPSEntityValue): Option[Contents] = {
    if (isNull(value)) None
    else
      value match {
        case node: CAPSNode => CAPSNode.contents(node)
        case rel: CAPSRelationship => CAPSRelationship.contents(rel)
      }
  }

  override def cypherType(value: CAPSEntityValue): CypherType =
    if (isNull(value)) CTNull
    else
      value match {
        case node: CAPSNode => CAPSNode.cypherType(node)
        case rel: CAPSRelationship => CAPSRelationship.cypherType(rel)
      }

  override def orderGroup(value: CAPSEntityValue): OrderGroup =
    if (isNull(value)) VoidOrderGroup
    else
      value match {
        case node: CAPSNode => CAPSNode.orderGroup(node)
        case rel: CAPSRelationship => CAPSRelationship.orderGroup(rel)
      }
}

sealed trait CAPSEntityCompanion[V <: CAPSEntityValue] extends CAPSMapCompanion[V] {

  override type Contents <: EntityContents
  override type Input = Contents

  def apply(value: Input): V = value match {
    case null => cypherNull
    case v => create(v)
  }

  // Entities are compared by id, therefore nulls in properties are not considered
  override def isOrContainsNull(v: V): Boolean = isNull(v)

  def id(v: V): Option[EntityId] = if (isNull(v)) None else Some(v.id)

  override protected[value] def computeOrder(l: V, r: V): Int =
    EntityId.ordering.compare(l.id, r.id)
}

sealed abstract class CAPSEntityValue(override protected[value] val properties: Properties)
  extends CAPSMap(properties) {

  protected[value] def id: EntityId

  protected[value] def data: EntityData
}

// *** NODE

case object CAPSNode extends CAPSEntityCompanion[CAPSNode] {

  override type Contents = NodeContents

  def apply(id: EntityId, data: NodeData = NodeData.empty): CAPSNode =
    apply(id, data.labels, data.properties)

  def apply(id: EntityId, labels: Seq[String], properties: Properties): CAPSNode =
    new CAPSNode(id, labels, properties)

  def create(contents: NodeContents): CAPSNode =
    apply(contents.id, contents.labels, contents.properties)

  override def contents(value: CAPSNode): Option[Contents] =
    if (value == null) None else Some(NodeContents(value.id, value.labels, value.properties))

  override def cypherType(value: CAPSNode): CypherType with DefiniteCypherType =
    if (value == null) CTNull else CTNode(labels(value).toSeq.flatten: _*)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CAPSNode): OrderGroup =
    if (isNull(value)) VoidOrderGroup else NodeOrderGroup

  def labels(node: CAPSNode): Option[Seq[String]] =
    if (isNull(node)) None else Some(node.labels)
}

sealed class CAPSNode(
                         protected[value] val id: EntityId,
                         // TODO: Use Set once available in Spark 2.3
                         protected[value] val labels: Seq[String],
                         override protected[value] val properties: Properties)
  extends CAPSEntityValue(properties)
    with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSNode => CAPSNode.equiv(this, other)
    case _ => false
  }

  override protected[value] def data = NodeData(labels, properties.m)

  override def toString = {
    val lbls = if (labels.isEmpty) "" else labels.mkString(":", ":", "")
    val props = if (properties.isEmpty) "" else super.toString
    Seq(lbls, props).filter(_.nonEmpty).mkString("(", " ", ")")
  }
}

// *** RELATIONSHIP

case object CAPSRelationship extends CAPSEntityCompanion[CAPSRelationship] {

  override type Contents = RelationshipContents

  def apply(id: EntityId, data: RelationshipData): CAPSRelationship =
    new CAPSRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)

  def apply(
             id: EntityId,
             startId: EntityId,
             endId: EntityId,
             relType: String,
             properties: Properties): CAPSRelationship =
    new CAPSRelationship(id, startId, endId, relType, properties)

  def create(contents: RelationshipContents): CAPSRelationship =
    apply(contents.id, contents.startId, contents.endId, contents.relationshipType, contents.properties)

  override def contents(value: CAPSRelationship): Option[RelationshipContents] =
    if (isNull(value))
      None
    else
      Some(RelationshipContents(value.id, value.startId, value.endId, value.relationshipType, value.properties))

  override def cypherType(value: CAPSRelationship): CypherType with DefiniteCypherType =
    if (isNull(value)) CTNull else relationshipType(value).map(t => CTRelationship(t)).getOrElse(CTRelationship)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CAPSRelationship): OrderGroup =
    if (isNull(value)) VoidOrderGroup else RelationshipOrderGroup

  def relationshipType(value: CAPSRelationship): Option[String] =
    if (isNull(value)) None else Some(value.relationshipType)

  def startId(value: CAPSRelationship): Option[EntityId] =
    if (isNull(value)) None else Some(value.startId)

  def endId(value: CAPSRelationship): Option[EntityId] =
    if (isNull(value)) None else Some(value.endId)
}

sealed class CAPSRelationship(
                                 protected[value] val id: EntityId,
                                 protected[value] val startId: EntityId,
                                 protected[value] val endId: EntityId,
                                 protected[value] val relationshipType: String,
                                 override protected[value] val properties: Properties)
  extends CAPSEntityValue(properties)
    with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSRelationship => CAPSRelationship.equiv(this, other)
    case _ => false
  }

  override protected[value] def data = RelationshipData(startId, endId, relationshipType, properties.m)

  override def toString = {
    val props = if (properties.isEmpty) "" else s" ${super.toString}"
    s"[:$relationshipType$props]"
  }
}

// *** Path

case object CAPSPath extends CAPSValueCompanion[CAPSPath] {

  override type Contents = Seq[CAPSEntityValue]
  override type Input = Contents

  def apply(value: Input): CAPSPath = value match {
    case null => cypherNull
    case v => create(v)
  }

  def create(contents: Seq[CAPSEntityValue]): CAPSPath =
  // TODO: Prevent construction of invalid paths, cf CypherTestValue suite for some initial code for this
    new CAPSPath(contents)

  override def cypherType(value: CAPSPath): CypherType =
    if (isNull(value)) CTNull else CTPath

  override def contents(value: CAPSPath): Option[Contents] =
    if (isNull(value)) None else Some(value.elements)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CAPSPath): OrderGroup =
    if (isNull(value)) VoidOrderGroup else PathOrderGroup

  override protected[value] def computeOrder(l: CAPSPath, r: CAPSPath): Int =
    pathOrdering.compare(l.elements, r.elements)

  private val pathOrdering = Ordering.Iterable(CAPSEntityCompanion.order)

  // A path containing null entities is malformed and therefore not considered here
  override def isOrContainsNull(v: CAPSPath): Boolean = isNull(v)
}

sealed class CAPSPath(protected[value] val elements: Seq[CAPSEntityValue]) extends CAPSValue with CypherPath with Serializable {

  override def hashCode(): Int = elements.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CAPSPath => CAPSPath.equiv(this, other)
    case _ => false
  }

  override def toString: String = s"<${elements.mkString(",")}>"
}
