package org.opencypher.spark.impl.newvalue

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark.api.CypherType.OrderGroup
import org.opencypher.spark.api.CypherType.OrderGroups._
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.{Ternary, _}
import org.opencypher.spark.impl.error.StdErrorInfo
import org.opencypher.spark.impl.verify.Verification

import scala.collection.SortedMap
import scala.language.implicitConversions

sealed trait CypherValueCompanion[V <: CypherValue] extends Equiv[V] {

  def cypherType(v: V): CypherType

  // scala value representation of this cypher value
  def scalaValue(v: V): Option[Any]

  def containsNull(v: V): Boolean = isNull(v)
  final def isNull(v: V): Boolean = v == null

  // Values in the same order group are ordered (sorted) together by orderability
  def orderGroup(v: V): OrderGroup

  // Cypher equivalence (also used by `==` on CypherValue instances)
  def equiv(l: V, r: V): Boolean =
    orderability(l, r) == 0

  // Cypher equality
  def equal(l: V, r: V): Ternary = {
    if (l eq r) {
      if (containsNull(l)) Maybe else True
    } else {
      if (containsNull(l) || containsNull(r))
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

  // Cypher orderability
  object orderability extends Ordering[V] with ((V, V) => Int) {
    override def apply(x: V, y: V): Int = compare(x, y)
    override def compare(x: V, y: V): Int = {
      if (x eq y) 0
      else if (y eq null) -1
      else if (null eq x) +1
      else computeOrderability(x,y)
    }
  }

  // Cypher comparability
  def comparability(x: V, y: V): Option[Int] = {
    // This is not a partial order (it is not reflexive!)
    if (x eq y) {
      if (containsNull(x)) None else Some(0)
    } else {
      if (containsNull(x) || containsNull(y))
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

  // compute orderability of non-null values from possibly different oder groups and possibly containing nested nulls
  protected[newvalue] def computeOrderability(l: V, r: V): Int

  // compare comparability of values from same order group and never being/containing nested nulls
  protected[newvalue] def computeComparability(l: V, r: V): Int
}


// *** ANY

case object CypherValue extends CypherValueCompanion[CypherValue] with Verification {

  import StdErrorInfo.Implicits._

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
  //
  //    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap =
  //      CypherMap(v.mapValues(ev))
  //
  //    implicit def cypherPath(v: Seq[CypherEntity]): CypherPath =
  //      CypherPath(v: _*)
  }

  trait LowPriorityConversion {
      implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
        v.mapValues(ev)

      implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
        v._1 -> v._2
  }

  object Encoders extends Encoders

  trait Encoders extends LowPriorityEncoders {
    //    implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
    //    implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] = kryo[CypherRelationship]
    //    implicit def cypherPathEncoder: ExpressionEncoder[CypherPath] = kryo[CypherPath]
    //
    //    implicit def cypherTuple1Encoder[T: ExpressionEncoder]: ExpressionEncoder[Tuple1[T]] =
    //      ExpressionEncoder.tuple(Seq(implicitly[ExpressionEncoder[T]])).asInstanceOf[ExpressionEncoder[Tuple1[T]]]
    //
    //    implicit def cypherTuple2Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder]: ExpressionEncoder[(T1, T2)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]]
    //      )
    //
    //    implicit def cypherTuple3Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]]
    //      )
    //
    //    implicit def cypherTuple4Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]],
    //        implicitly[ExpressionEncoder[T4]]
    //      )
    //
    //    implicit def cypherTuple5Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder, T5: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]],
    //        implicitly[ExpressionEncoder[T4]],
    //        implicitly[ExpressionEncoder[T5]]
    //      )
    //  }
  }

  trait LowPriorityEncoders {
    implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
    implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
//    implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
  }

  implicit def companion[V <: CypherValue : CypherValueCompanion] = implicitly[CypherValueCompanion[V]]

  object Companions extends Companions

  trait Companions {
    implicit def cypherBooleanCompanion: CypherBoolean.type = CypherBoolean
    implicit def cypherStringCompanion: CypherString.type = CypherString
    implicit def cypherFloatCompanion: CypherFloat.type = CypherFloat
    implicit def cypherIntegerCompanion: CypherInteger.type = CypherInteger
    implicit def cypherNumberCompanion: CypherNumber.type = CypherNumber
    implicit def cypherListCompanion: CypherList.type = CypherList
    implicit def cypherMapCompanion: CypherMap.type = CypherMap
    implicit def cypherValueCompanion: CypherValue.type = CypherValue
  }

  override def cypherType(value: CypherValue): CypherType = value match {
    case null             => CTNull
    case v: CypherBoolean => CypherBoolean.cypherType(v)
    case v: CypherString  => CypherString.cypherType(v)
    case v: CypherNumber  => CypherNumber.cypherType(v)
    case v: CypherList    => CypherList.cypherType(v)
    case v: CypherMap     => CypherMap.cypherType(v)
  }

  def unapply(value: CypherValue): Option[Any] = scalaValue(value)

  override def scalaValue(value: CypherValue): Option[Any] = value match {
    case null              => None
    case v: CypherBoolean  => CypherBoolean.scalaValue(v)
    case v: CypherString   => CypherString.scalaValue(v)
    case v: CypherNumber   => CypherNumber.scalaValue(v)
    case v: CypherList     => CypherList.scalaValue(v)
    case v: CypherMap      => CypherMap.scalaValue(v)
  }

  override def orderGroup(value: CypherValue) = value match {
    case null             => VoidOrderGroup
    case v: CypherBoolean => CypherBoolean.orderGroup(v)
    case v: CypherString  => CypherString.orderGroup(v)
    case v: CypherNumber  => CypherNumber.orderGroup(v)
    case v: CypherList    => CypherList.orderGroup(v)
    case v: CypherMap     => CypherMap.orderGroup(v)
  }

  protected[newvalue] def computeOrderability(l: CypherValue, r: CypherValue): Int = {
    val lGroup = orderGroup(l)
    val rGroup = orderGroup(r)
    val cmp = lGroup.id - rGroup.id
    if (cmp == 0)
      (l, r) match {
        case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.computeOrderability(a, b)
        case (a: CypherString, b: CypherString)   => CypherString.computeOrderability(a, b)
        case (a: CypherNumber, b: CypherNumber)   => CypherNumber.computeOrderability(a, b)
        case (a: CypherList, b: CypherList)       => CypherList.computeOrderability(a, b)
        case (a: CypherMap, b: CypherMap)         => CypherMap.computeOrderability(a, b)
        case _ =>
          supposedlyImpossible("Call to computeOrderability with values of different types")
      }
    else
      cmp
  }

  override protected[newvalue] def computeComparability(l: CypherValue, r: CypherValue): Int = (l, r) match {
    case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.computeComparability(a, b)
    case (a: CypherString, b: CypherString) => CypherString.computeComparability(a, b)
    case (a: CypherNumber, b: CypherNumber) => CypherNumber.computeComparability(a, b)
    case (a: CypherList, b: CypherList) => CypherList.computeComparability(a, b)
    case (a: CypherMap, b: CypherMap) => CypherMap.computeComparability(a, b)
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
  override def scalaValue(value: CypherBoolean) = unapply(value).map(boolean2Boolean)

  def orderGroup(v: CypherBoolean) = if (v == null) VoidOrderGroup else BooleanOrderGroup

  override protected[newvalue] def computeOrderability(l: CypherBoolean, r: CypherBoolean): Int =
    Ordering.Boolean.compare(l.v, r.v)

  override protected[newvalue] def computeComparability(l: CypherBoolean, r: CypherBoolean): Int =
    computeOrderability(l, r)
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
  override def scalaValue(value: CypherString) = unapply(value)

  def orderGroup(v: CypherString) = if (v == null) VoidOrderGroup else StringOrderGroup

  override protected[newvalue] def computeOrderability(l: CypherString, r: CypherString): Int =
    Ordering.String.compare(l.v, r.v)

  override protected[newvalue] def computeComparability(l: CypherString, r: CypherString): Int =
    computeOrderability(l, r)
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
  def scalaValue(v: V): Option[Number]

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

  override def scalaValue(v: CypherNumber): Option[Number] = unapply(v)

  override protected[newvalue] def computeOrderability(l: CypherNumber, r: CypherNumber): Int = (l, r) match {
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

  override protected[newvalue] def computeComparability(l: CypherNumber, r: CypherNumber): Int =
    computeOrderability(l, r)

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
  override def scalaValue(value: CypherInteger) = unapply(value).map(long2Long)

  override protected[newvalue] def computeOrderability(l: CypherInteger, r: CypherInteger): Int =
    Ordering.Long.compare(l.v, r.v)

  override protected[newvalue] def computeComparability(l: CypherInteger, r: CypherInteger): Int =
    computeOrderability(l, r)
}

final class CypherInteger(private[CypherInteger] val v: Long) extends CypherNumber with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherInteger => CypherInteger.equiv(this, other)
    case other: CypherNumber  => CypherNumber.equiv(this, other)
    case _                    => false
  }

  override def toString: String = s"$v :: INTEGER"
}


// *** FLOAT ***

case object CypherFloat extends CypherNumberCompanion[CypherFloat] {
  def apply(value: Double): CypherFloat = new CypherFloat(value)
  def unapply(value: CypherFloat): Option[Double] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherFloat) = if (value == null) CTNull else CTFloat
  override def scalaValue(value: CypherFloat) = unapply(value).map(double2Double)

  override protected[newvalue] def computeOrderability(l: CypherFloat, r: CypherFloat): Int = {
    val lVal = l.v
    val rVal = r.v
    if (lVal.isNaN) {
      if (rVal.isNaN) 0 else +1
    } else {
      if (rVal.isNaN) -1 else Ordering.Double.compare(lVal, rVal)
    }
  }

  override protected[newvalue] def computeComparability(l: CypherFloat, r: CypherFloat): Int =
    computeOrderability(l, r)
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
  def apply(value: Seq[CypherValue]): CypherList = new CypherList(value)
  def unapply(value: CypherList): Option[Seq[CypherValue]] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherList) =
    if (value == null) CTNull else CTList(value.cachedElementType)

  override def scalaValue(value: CypherList) =
    unapply(value)


  override def containsNull(v: CypherList): Boolean =
    isNull(v) || v.cachedContainsNull

  override protected[newvalue] def computeOrderability(l: CypherList, r: CypherList): Int =
    valueListOrderability.compare(l.v, r.v)

  override protected[newvalue] def computeComparability(l: CypherList, r: CypherList): Int =
    computeOrderability(l, r)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(v: CypherList): OrderGroup = ListOrderGroup

  private val valueListOrderability = Ordering.Iterable(CypherValue.orderability)
}

final class CypherList(private[CypherList] val v: Seq[CypherValue])
  extends CypherValue with Serializable {

  @transient
  private[CypherList] lazy val cachedContainsNull: Boolean =
    v.exists(CypherValue.containsNull)

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

case object CypherMap extends CypherValueCompanion[CypherMap] {

  object empty extends CypherMap(Properties.empty)

  def apply(elts: (String, CypherValue)*): CypherMap =
    if (elts.isEmpty) apply(Properties.empty) else new CypherMap(Properties(elts: _*))

  def apply(value: Properties): CypherMap =
    if (value == Properties.empty) empty else new CypherMap(value)

  def unapply(value: CypherMap): Option[Properties] =
    if (value == null) None else Some(value.properties)

  override def cypherType(value: CypherMap) =
    if (value == null) CTNull else CTMap

  override def scalaValue(value: CypherMap): Option[Properties] =
    unapply(value)

  override def containsNull(v: CypherMap): Boolean =
    isNull(v) || v.cachedContainsNull

  override protected[newvalue] def computeOrderability(l: CypherMap, r: CypherMap): Int =
    mapEntryOrdering.compare(l.properties.m, r.properties.m)

  override protected[newvalue] def computeComparability(l: CypherMap, r: CypherMap): Int =
    computeOrderability(l, r)

  // Values in the same order group are ordered (sorted) together by orderability
  override def orderGroup(v: CypherMap): OrderGroup = MapOrderGroup

  private val mapEntryOrdering = Ordering.Iterable(Ordering.Tuple2(Ordering.String, CypherValue.orderability))
}

sealed class CypherMap(private[CypherMap] val properties: Properties)
  extends CypherValue with Serializable {

  @transient
  private[CypherMap] lazy val cachedContainsNull: Boolean =
    properties.m.values.exists(CypherValue.containsNull)

  override def hashCode(): Int = properties.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherMap  => CypherMap.equiv(this, other)
    case _                 => false
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

    builder.append(']')
    builder.result()
  }
}

