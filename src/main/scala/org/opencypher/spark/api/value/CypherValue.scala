/**
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
package org.opencypher.spark.api.value

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark_legacy.api._
import org.opencypher.spark_legacy.impl.verify.Verification
import org.opencypher.spark.api.types.CypherType._
import org.opencypher.spark.api.types.CypherType.OrderGroups._
import org.opencypher.spark.api.types._

import scala.collection.TraversableOnce
import scala.language.implicitConversions

object CypherValueCompanion {
  implicit def cypherValueCompanion: CypherValue.type = CypherValue
  implicit def cypherBooleanCompanion: CypherBoolean.type = CypherBoolean
  implicit def cypherStringCompanion: CypherString.type = CypherString
  implicit def cypherFloatCompanion: CypherFloat.type = CypherFloat
  implicit def cypherIntegerCompanion: CypherInteger.type = CypherInteger
  implicit def cypherNumberCompanion: CypherNumber.type = CypherNumber
  implicit def cypherListCompanion: CypherList.type = CypherList
  implicit def cypherMapCompanion: CypherMap.type = CypherMap
  implicit def cypherNodeCompanion: CypherNode.type = CypherNode
  implicit def cypherRelationshipCompanion: CypherRelationship.type = CypherRelationship
  implicit def cypherPathCompanion: CypherPath.type = CypherPath
}

sealed trait CypherValueCompanion[V <: CypherValue] extends Equiv[V] {

  def cypherType(v: V): CypherType

  // Scala value representation of this cypher value
  def contents(v: V): Option[Any]

  def isIncomparable(v: V): Boolean = isNull(v)
  final def isNull(v: V): Boolean = v == null

  // Values in the same order group are ordered (sorted) together by orderability
  def orderGroup(v: V): OrderGroup

  // Cypher equivalence (also used by `==` on CypherValue instances)
  def equiv(l: V, r: V): Boolean =
    orderability(l, r) == 0

  // Cypher equality
  def equal(l: V, r: V): Ternary = {
    if (l eq r) {
      if (isIncomparable(l)) Maybe else True
    } else {
      if (isIncomparable(l) || isIncomparable(r))
        Maybe
      else {
        val xGroup = orderGroup(l)
        val yGroup = orderGroup(r)
        if (xGroup == yGroup)
          Ternary(computeComparability(l, r) == 0)
        else
          False
      }
    }
  }

  final val reverseOrderability = orderability.reverse

  // Cypher orderability
  implicit object orderability extends Ordering[V] with ((V, V) => Int) {
    override def apply(x: V, y: V): Int = compare(x, y)
    override def compare(x: V, y: V): Int = {
      if (x eq y) 0
      else if (y eq null) -1
      else if (null eq x) +1
      else computeOrderability(x, y)
    }
  }

  // Cypher comparability
  def comparability(x: V, y: V): Option[Int] = {
    // This is not a partial order (it is not reflexive!)
    if (x eq y) {
      if (isIncomparable(x)) None else Some(0)
    } else {
      if (isIncomparable(x) || isIncomparable(y))
        None
      else {
        val xGroup = orderGroup(x)
        val yGroup = orderGroup(y)
        if (xGroup == yGroup)
          Some(computeComparability(x, y))
        else
          None
      }
    }
  }

  // Compute orderability of non-null values from possibly different order groups and possibly containing nested nulls
  protected[value] def computeOrderability(l: V, r: V): Int

  // Compute comparability of values from same order group that are never null or contain nested nulls
  protected[value] def computeComparability(l: V, r: V): Int =
    computeOrderability(l, r)
}


// *** ANY

case object CypherValue extends CypherValueCompanion[CypherValue] with Verification {

  import org.opencypher.spark_legacy.impl.error.StdErrorInfo.Implicits._

  import scala.collection.JavaConverters._

  // TODO: Complete and test
  // TODO: Support scala types
  def apply(v: Any): CypherValue = v match {
    case v: CypherValue => v
    case v: String => CypherString(v)
    case v: java.lang.Byte => CypherInteger(v.toLong)
    case v: java.lang.Short => CypherInteger(v.toLong)
    case v: java.lang.Integer => CypherInteger(v.toLong)
    case v: java.lang.Long => CypherInteger(v)
    case v: java.lang.Float => CypherFloat(v.toDouble)
    case v: java.lang.Double => CypherFloat(v)
    case v: java.lang.Boolean => CypherBoolean(v)
    case v: java.util.Map[_, _] if v.isEmpty => CypherMap.empty
    case v: java.util.Map[_, _] => CypherMap(v.asScala.collect { case (k,v) => k.toString -> apply(v) }.toMap)
    case v: java.util.List[_] if v.isEmpty => CypherList.empty
    case v: java.util.List[_] => CypherList(v.asScala.map(apply))
    case v: Array[_] => CypherList(v.map(apply))
    case null => null
    case x => throw new IllegalArgumentException(s"Unexpected property value: $x")
  }

  object Conversion extends Conversion

  trait Conversion extends LowPriorityConversion {

    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)
    implicit def cypherString(v: String): CypherString = CypherString(v)
    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
    implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)

    implicit def cypherTernary(v: Ternary): CypherValue =
      if (v.isDefinite) CypherBoolean(v.isTrue) else cypherNull

    implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
      v.map(ev).getOrElse(cypherNull)

    implicit def cypherList[T](v: IndexedSeq[T])(implicit ev: T => CypherValue): CypherList =
      CypherList(v.map(ev))

    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap =
      CypherMap(v.mapValues(ev))
  }

  trait LowPriorityConversion {
      implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
        v.mapValues(ev)

      implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
        v._1 -> v._2
  }

  object Encoders extends Encoders

  trait Encoders extends LowPriorityEncoders {
      implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
      implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] = kryo[CypherRelationship]
  }

  trait LowPriorityEncoders {
    implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
    implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
    implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
  }

  def companion[V <: CypherValue : CypherValueCompanion] = implicitly[CypherValueCompanion[V]]

  def unapply(value: CypherValue): Option[Any] = contents(value)

  override def cypherType(value: CypherValue): CypherType = value match {
    case null                   => CTNull
    case v: CypherBoolean       => CypherBoolean.cypherType(v)
    case v: CypherString        => CypherString.cypherType(v)
    case v: CypherNumber        => CypherNumber.cypherType(v)
    case v: CypherList          => CypherList.cypherType(v)
    case v: CypherMap           => CypherMap.cypherType(v)
    case v: CypherPath          => CypherPath.cypherType(v)
  }

  override def contents(value: CypherValue): Option[Any] = value match {
    case null                      => None
    case v: CypherBoolean          => CypherBoolean.contents(v)
    case v: CypherString           => CypherString.contents(v)
    case v: CypherNumber           => CypherNumber.contents(v)
    case v: CypherList             => CypherList.contents(v)
    case v: CypherMap              => CypherMap.contents(v)
    case v: CypherPath             => CypherPath.contents(v)
  }

  override def orderGroup(value: CypherValue) = value match {
    case null                     => VoidOrderGroup
    case v: CypherBoolean         => CypherBoolean.orderGroup(v)
    case v: CypherString          => CypherString.orderGroup(v)
    case v: CypherNumber          => CypherNumber.orderGroup(v)
    case v: CypherList            => CypherList.orderGroup(v)
    case v: CypherMap             => CypherMap.orderGroup(v)
    case v: CypherPath            => CypherPath.orderGroup(v)
  }

  protected[value] def computeOrderability(l: CypherValue, r: CypherValue): Int = {
    val lGroup = orderGroup(l)
    val rGroup = orderGroup(r)
    val cmp = lGroup.id - rGroup.id
    if (cmp == 0)
      (l, r) match {
        case (a: CypherBoolean, b: CypherBoolean)            => CypherBoolean.computeOrderability(a, b)
        case (a: CypherString, b: CypherString)              => CypherString.computeOrderability(a, b)
        case (a: CypherNumber, b: CypherNumber)              => CypherNumber.computeOrderability(a, b)
        case (a: CypherList, b: CypherList)                  => CypherList.computeOrderability(a, b)
        case (a: CypherMap, b: CypherMap)                    => CypherMap.computeOrderability(a, b)
        case (a: CypherPath, b: CypherPath)                  => CypherPath.computeOrderability(a, b)
        case _ =>
          supposedlyImpossible("Call to computeOrderability with values of different types")
      }
    else
      cmp
  }

  override protected[value] def computeComparability(l: CypherValue, r: CypherValue): Int = (l, r) match {
    case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.computeComparability(a, b)
    case (a: CypherString, b: CypherString) => CypherString.computeComparability(a, b)
    case (a: CypherNumber, b: CypherNumber) => CypherNumber.computeComparability(a, b)
    case (a: CypherList, b: CypherList) => CypherList.computeComparability(a, b)
    case (a: CypherMap, b: CypherMap) => CypherMap.computeComparability(a, b)
    case (a: CypherPath, b: CypherPath) => CypherPath.computeComparability(a, b)
    case _ =>
      supposedlyImpossible("Call to computeComparability with values of different types")
  }
}

sealed trait CypherValue {
  self: Serializable =>
}


// *** BOOLEAN

case object CypherBoolean extends CypherValueCompanion[CypherBoolean] {
  val TRUE = new CypherBoolean(true)
  val FALSE = new CypherBoolean(false)

  def apply(value: Boolean): CypherBoolean = if (value) TRUE else FALSE
  def unapply(value: CypherBoolean): Option[Boolean] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherBoolean) = if (value == null) CTNull else CTBoolean
  override def contents(value: CypherBoolean) = unapply(value).map(boolean2Boolean)

  def orderGroup(v: CypherBoolean) = if (v == null) VoidOrderGroup else BooleanOrderGroup

  override protected[value] def computeOrderability(l: CypherBoolean, r: CypherBoolean): Int =
    Ordering.Boolean.compare(l.v, r.v)
}

final class CypherBoolean(private[CypherBoolean] val v: Boolean) extends CypherValue with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherBoolean => CypherBoolean.equiv(this, other)
    case _                    => false
  }

  override def toString: String = if (v) "TRUE" else "FALSE"
}


// *** STRING

case object CypherString extends CypherValueCompanion[CypherString] {
  def apply(value: String): CypherString = if (value == null) cypherNull else new CypherString(value)
  def unapply(value: CypherString): Option[String] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherString) = if (value == null) CTNull else CTString
  override def contents(value: CypherString) = unapply(value)

  def orderGroup(v: CypherString) = if (v == null) VoidOrderGroup else StringOrderGroup

  override protected[value] def computeOrderability(l: CypherString, r: CypherString): Int =
    Ordering.String.compare(l.v, r.v)
}

final class CypherString(private[CypherString] val v: String) extends CypherValue with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherString => CypherString.equiv(this, other)
    case _                   => false
  }

  // TODO: Improve
  override def toString: String = s"'${v.replaceAllLiterally("'", "\'")}'"
}


// *** NUMBER

sealed trait CypherNumberCompanion[V <: CypherNumber] extends CypherValueCompanion[V] {
  def contents(v: V): Option[Number]

  def orderGroup(v: V) = if (v == null) VoidOrderGroup else NumberOrderGroup
}

case object CypherNumber extends CypherNumberCompanion[CypherNumber] {

  def unapply(value: CypherNumber): Option[Number] = value match {
    case CypherInteger(v) => Some(v)
    case CypherFloat(v)   => Some(v)
    case _                => None
  }

  override def cypherType(value: CypherNumber): CypherType =
    if (value == null) CTNull else value match {
      case v: CypherInteger => CypherInteger.cypherType(v)
      case v: CypherFloat   => CypherFloat.cypherType(v)
    }

  override def contents(v: CypherNumber): Option[Number] = unapply(v)

  override protected[value] def computeOrderability(l: CypherNumber, r: CypherNumber): Int = (l, r) match {
    case (a: CypherInteger, b: CypherInteger) =>
      CypherInteger.computeOrderability(a, b)

    case (a: CypherFloat, b: CypherFloat) =>
      CypherFloat.computeOrderability(a, b)

    case (a@CypherFloat(f), b@CypherInteger(i)) =>
      if (fitsDouble(i)) CypherFloat.computeOrderability(a, CypherFloat(i))
      else BigDecimal.decimal(f).compare(BigDecimal.decimal(i))

    case (a@CypherInteger(i), b@CypherFloat(f)) =>
      if (fitsDouble(i)) CypherFloat.computeOrderability(CypherFloat(i), b)
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
  def apply(value: Long): CypherInteger = new CypherInteger(value)
  def unapply(value: CypherInteger): Option[Long] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherInteger) = if (value == null) CTNull else CTInteger
  override def contents(value: CypherInteger) = unapply(value).map(long2Long)

  override protected[value] def computeOrderability(l: CypherInteger, r: CypherInteger): Int =
    Ordering.Long.compare(l.v, r.v)
}

final class CypherInteger(private val v: Long) extends CypherNumber with Serializable {
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
  def apply(value: Double): CypherFloat = new CypherFloat(value)
  def unapply(value: CypherFloat): Option[Double] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherFloat) = if (value == null) CTNull else CTFloat
  override def contents(value: CypherFloat) = unapply(value).map(double2Double)

  override protected[value] def computeOrderability(l: CypherFloat, r: CypherFloat): Int = {
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

  override def toString: String = s"$v :: FLOAT"
}


// *** LIST

case object CypherList extends CypherValueCompanion[CypherList] {
  val empty = CypherList(Seq.empty)

  def apply(value: Seq[CypherValue]): CypherList = new CypherList(value)
  def unapply(value: CypherList): Option[Seq[CypherValue]] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherList) =
    if (value == null) CTNull else CTList(value.cachedElementType)

  override def contents(value: CypherList) =
    unapply(value)


  override def isIncomparable(v: CypherList): Boolean =
    isNull(v) || v.cachedContainsNull

  override protected[value] def computeOrderability(l: CypherList, r: CypherList): Int =
    valueListOrderability.compare(l.v, r.v)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(v: CypherList): OrderGroup = ListOrderGroup

  private val valueListOrderability = Ordering.Iterable(CypherValue.orderability)
}

final class CypherList(private[CypherList] val v: Seq[CypherValue])
  extends CypherValue with Serializable {

  def mapToTraversable[B](f: CypherValue => B): TraversableOnce[B] = {
    v.map(f)
  }

  @transient
  private[CypherList] lazy val cachedContainsNull: Boolean =
    v.exists(CypherValue.isIncomparable)

  @transient
  private[CypherList] lazy val cachedElementType: CypherType =
    v.map(CypherValue.cypherType).reduceOption(_ join _).getOrElse(CTVoid)

  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherList  => CypherList.equiv(this, other)
    case _                  => false
  }

  // TODO: Test all the toStrings
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

  def property(value: V)(name: String): CypherValue =
    if (value == null) null else value.properties(name)

  def properties(value: V): Option[Properties] =
    if (value == null) None else Some(value.properties)

  override def isIncomparable(v: V): Boolean =
    isNull(v) || v.cachedIsComparable
}

case object CypherMap extends CypherMapCompanion[CypherMap] {

  object empty extends CypherMap(Properties.empty)

  def apply(elts: (String, CypherValue)*): CypherMap =
    if (elts.isEmpty) apply(Properties.empty) else new CypherMap(Properties(elts: _*))

  def apply(value: Properties): CypherMap =
    if (value == Properties.empty) empty else new CypherMap(value)

  def unapply(value: CypherMap): Option[Properties] =
    if (value == null) None else Some(value.properties)

  override def cypherType(value: CypherMap) =
    if (value == null) CTNull else value match {
      case entity: CypherEntityValue => CypherEntityCompanion.cypherType(entity)
      case _ => CTMap
    }

  override def contents(value: CypherMap): Option[Any] =
    if (value == null) None else value match {
      case entity: CypherEntityValue => CypherEntityCompanion.contents(entity)
      case _ => properties(value)
    }

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(value: CypherMap): OrderGroup =
    if (value == null) VoidOrderGroup else value match {
      case entity: CypherEntityValue => CypherEntityCompanion.orderGroup(entity)
      case _ => MapOrderGroup
    }

  protected[value] def computeOrderability(l: CypherMap, r: CypherMap): Int = (l, r) match {
    case (a: CypherEntityValue, b: CypherEntityValue) => CypherEntityCompanion.computeOrderability(a, b)
    case (a: CypherMap, b: CypherMap) => mapEntryOrdering.compare(l.properties.m, r.properties.m)
  }

  private val mapEntryOrdering =
    Ordering.Iterable(Ordering.Tuple2(Ordering.String, CypherValue.orderability))
}

sealed class CypherMap(protected[value] val properties: Properties)
  extends CypherValue with Serializable {

  def get(key: String): Option[CypherValue] = properties.get(key)

  @transient
  protected[value] lazy val cachedIsComparable: Boolean =
    properties.m.values.exists(CypherValue.isIncomparable)

  override def hashCode(): Int = properties.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherEntityValue => false
    case other: CypherMap => CypherMap.equiv(this, other)
    case _ => false
  }

  // TODO: Test all the toStrings, possibly move to companion
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

  override def contents(value: CypherEntityValue): Option[(EntityId, EntityData)] = {
    if (value == null) None
    else value match {
      case node: CypherNode => CypherNode.contents(node)
      case rel: CypherRelationship => CypherRelationship.contents(rel)
    }
  }

  override def cypherType(value: CypherEntityValue): CypherType =
    if (value == null) CTNull
    else value match {
      case node: CypherNode => CypherNode.cypherType(node)
      case rel: CypherRelationship => CypherRelationship.cypherType(rel)
    }

  override def orderGroup(value: CypherEntityValue): OrderGroup =
    if (value == null) VoidOrderGroup
    else value match {
      case node: CypherNode => CypherNode.orderGroup(node)
      case rel: CypherRelationship => CypherRelationship.orderGroup(rel)
    }
}

sealed trait CypherEntityCompanion[V <: CypherEntityValue] extends CypherMapCompanion[V] {

  override def isIncomparable(v: V): Boolean = isNull(v)

  override def contents(value: V): Option[(EntityId, EntityData)]

  def id(v: V): Option[EntityId] = if (v == null) None else Some(v.id)

  override protected[value] def computeOrderability(l: V, r: V): Int =
    EntityId.ordering.compare(l.id, r.id)
}

sealed abstract class CypherEntityValue(override protected[value] val properties: Properties)
  extends CypherMap(properties) {

  protected[value] def id: EntityId
  protected[value] def data: EntityData
}

// *** NODE

case object CypherNode extends CypherEntityCompanion[CypherNode] {

  def apply(id: EntityId, labels: Array[String], properties: Properties): CypherNode =
    new CypherNode(id, labels, properties)

  def unapply(value: CypherNode): Option[(EntityId, Array[String], Properties)] =
    if (value == null) None else Some((value.id, value.labels, value.properties))

  override def contents(value: CypherNode): Option[(EntityId, NodeData)] =
    if (value == null) None else Some(value.id -> value.data)

  override def cypherType(value: CypherNode) =
    if (value == null) CTNull else CTNode

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(v: CypherNode): OrderGroup = NodeOrderGroup

  def labels(node: CypherNode): Option[Seq[String]] = if (node == null) None else Some(node.labels)
}

sealed class CypherNode(protected[value] val id: EntityId,
                        protected[value] val labels: Array[String],
                        override protected[value] val properties: Properties)
  extends CypherEntityValue(properties) with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherNode => CypherNode.equiv(this, other)
    case _                 => false
  }

  override protected[value] def data = NodeData(labels, properties.m)

  override def toString = s"($id${labels.map(":" + _).foldLeft("")(_ ++ _)} ${super.toString})"
}

// *** RELATIONSHIP

case object CypherRelationship extends CypherEntityCompanion[CypherRelationship] {

  def apply(id: EntityId, data: RelationshipData) =
    new CypherRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)

  def apply(id: EntityId, startId: EntityId, endId: EntityId, relType: String, properties: Properties): CypherRelationship =
    new CypherRelationship(id, startId, endId, relType, properties)

  def unapply(value: CypherRelationship): Option[(EntityId, EntityId, EntityId, String, Properties)] =
    if (value == null) None else Some((value.id, value.startId, value.endId, value.relationshipType, value.properties))

  override def contents(value: CypherRelationship): Option[(EntityId, RelationshipData)] =
    if (value == null) None else Some(value.id -> value.data)

  override def cypherType(value: CypherRelationship) =
    if (value == null) CTNull else CTRelationship

  override def orderGroup(v: CypherRelationship): OrderGroup = RelationshipOrderGroup

  def relationshipType(relationship: CypherRelationship): Option[String] =
    if (relationship == null) None else Some(relationship.relationshipType)

  def startId(relationship: CypherRelationship): Option[EntityId] =
    if (relationship == null) None else Some(relationship.startId)

  def endId(relationship: CypherRelationship): Option[EntityId] =
    if (relationship == null) None else Some(relationship.endId)
}

sealed class CypherRelationship(protected[value] val id: EntityId,
                                protected[value] val startId: EntityId,
                                protected[value] val endId: EntityId,
                                protected[value] val relationshipType: String,
                                override protected[value] val properties: Properties)
  extends CypherEntityValue(properties) with Serializable {

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherRelationship => CypherRelationship.equiv(this, other)
    case _                         => false
  }

  override protected[value] def data = RelationshipData(startId, relationshipType, endId, properties.m)

  override def toString = s"($startId)-[$id:$relationshipType ${super.toString}]->($endId)"
}

// *** Path

case object CypherPath extends CypherValueCompanion[CypherPath] {

  def apply(elements: CypherEntityValue*): CypherPath = {
    // TODO: Prevent construction of invalid paths, cf CypherTestValue suite for some initial code for this
    new CypherPath(elements)
  }

  override def cypherType(v: CypherPath): CypherType = if (v == null) CTNull else CTPath

  def unapply(path: CypherPath): Option[Seq[CypherEntityValue]] =
    if (path == null) None else Some(path.elements)

  override def contents(v: CypherPath): Option[Any] = unapply(v)

  override def orderGroup(v: CypherPath): OrderGroup = PathOrderGroup

  override protected[value] def computeOrderability(l: CypherPath, r: CypherPath): Int =
    pathOrdering.compare(l.elements, r.elements)

  private val pathOrdering = Ordering.Iterable(CypherEntityCompanion.orderability)
}

sealed class CypherPath(protected[value] val elements: Seq[CypherEntityValue])
  extends CypherValue with Serializable {

  override def hashCode(): Int = elements.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherPath => CypherPath.equiv(this, other)
    case _                 => false
  }

  override def toString: String = s"<${elements.mkString(",")}>"
}
