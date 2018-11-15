/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.api.types

import cats.Monoid
import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.types.CypherTypeParser
import upickle.default._

import scala.language.postfixOps

object CypherType {

  implicit def rw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => fromName(s).get)

  /**
    * Parses the name of CypherType into the actual CypherType object.
    *
    * @param name string representation of the CypherType
    * @return
    * @see {{{org.opencypher.okapi.api.types.CypherType#name}}}
    */
  def fromName(name: String): Option[CypherType] = {
   CypherTypeParser.parse(name)
  }

  implicit class TypeCypherValue(cv: CypherValue) {
    def cypherType: CypherType = {
      cv match {
        case CypherNull => CTNull
        case CypherBoolean(_) => CTBoolean
        case CypherFloat(_) => CTFloat
        case CypherInteger(_) => CTInteger
        case CypherString(_) => CTString
        case CypherMap(inner) => CTMap(inner.mapValues(_.cypherType))
        case CypherNode(_, labels, _) => CTNode(labels)
        case CypherRelationship(_, _, _, relType, _) => CTRelationship(relType)
        case CypherList(l) => CTList(l.map(_.cypherType).foldLeft[CypherType](CTVoid)(_.join(_)))
      }
    }
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x join y
  }

}

case object CTAny extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
  override def name = "ANY"

  override def superTypeOf(other: CypherType): Ternary = !other.isNullable

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = this

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other
}

case object CTBoolean extends MaterialDefiniteCypherLeafType {
  override def name = "BOOLEAN"
}

case object CTNumber extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def name = "NUMBER"

  override def superTypeOf(other: CypherType): Ternary = other match {
    case CTNumber => True
    case CTInteger => True
    case CTFloat => True
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNumber => self
    case CTInteger => self
    case CTFloat => self
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }
}

case object CTInteger extends MaterialDefiniteCypherLeafType {

  self =>

  override def name = "INTEGER"

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNumber => CTNumber
    case CTInteger => self
    case CTFloat => CTNumber
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }
}

case object CTFloat extends MaterialDefiniteCypherLeafType {

  self =>

  override def name = "FLOAT"

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNumber => CTNumber
    case CTInteger => CTNumber
    case CTFloat => self
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }
}

case object CTString extends MaterialDefiniteCypherLeafType {
  override def name = "STRING"
}

case class CTMap(innerTypes: Map[String, CypherType]) extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
  override def name = {
    val innerNames = innerTypes.map {
      case(key, valueType) => s"$key: ${valueType.name}"
    }.mkString(", ")

    s"MAP($innerNames)"
  }

  override def superTypeOf(other: CypherType): Ternary = other match {
    case CTMap(otherInner) if innerTypes.keySet == otherInner.keySet =>
      innerTypes.forall { case (key, value) => value.superTypeOf(otherInner(key)).isTrue }
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTMap(otherInnerTypes: Map[String, CypherType]) => CTMap(innerTypes |+| otherInnerTypes)
    case CTVoid => this
    case CTWildcard => CTWildcard
    case _ => CTAny
  }
}

object CTNode extends CTNode(Set.empty, None) with Serializable {
  def apply(labels: String*): CTNode =
    CTNode(labels.toSet)

  def apply(labels: Set[String]): CTNode =
    CTNode(labels, None)
}

sealed case class CTNode(
  labels: Set[String],
  override val graph: Option[QualifiedGraphName]
) extends MaterialDefiniteCypherType {

  self =>

  private def graphToString = graph.map(n => s" @ $n").getOrElse("")

  final override def name: String =
    if (labels.isEmpty) s"NODE$graphToString" else s"NODE(${labels.mkString(":", ":", "")})$graphToString"

  final override def nullable: CTNodeOrNull = CTNodeOrNull(labels, graph)

  final override def superTypeOf(other: CypherType): Ternary = other match {
    case CTNode(otherLabels, _) => Ternary(labels subsetOf otherLabels)
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  final override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNode(otherLabels, qgnOpt) => CTNode(labels intersect otherLabels, if (graph == qgnOpt) graph else None)
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  final override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNode(otherLabels, qgnOpt) => CTNode(labels union otherLabels, if (graph == qgnOpt) graph else None)
    case _ => super.meetMaterially(other)
  }

  override def withGraph(qgn: QualifiedGraphName): CypherType = copy(graph = Some(qgn))
}

object CTNodeOrNull extends CTNodeOrNull(Set.empty, None) with Serializable {
  def apply(labels: String*): CTNodeOrNull =
    CTNodeOrNull(labels.toSet)

  def apply(labels: Set[String]): CTNodeOrNull =
    CTNodeOrNull(labels, None)
}

sealed case class CTNodeOrNull(
  labels: Set[String],
  override val graph: Option[QualifiedGraphName]
) extends NullableDefiniteCypherType {
  final override def name = s"$material?"

  final override def material: CTNode = CTNode(labels, graph)
}

object CTRelationship extends CTRelationship(Set.empty, None) with Serializable {
  def apply(types: String*): CTRelationship =
    CTRelationship(types.toSet)

  def apply(types: Set[String]): CTRelationship =
    CTRelationship(types, None)
}

sealed case class CTRelationship(
  types: Set[String],
  override val graph: Option[QualifiedGraphName]
) extends MaterialDefiniteCypherType {

  self =>

  private def graphToString = graph.map(n => s" @ $n").getOrElse("")

  final override def name: String =
    if (types.isEmpty) s"RELATIONSHIP$graphToString" else s"RELATIONSHIP(${
      types.map(t => s"$t").mkString(":", "|:", "")
    })$graphToString"

  final override def nullable: CTRelationshipOrNull =
    CTRelationshipOrNull(types, graph)

  final override def superTypeOf(other: CypherType): Ternary = other match {
    case CTRelationship(_, _) if types.isEmpty => True
    case CTRelationship(otherTypes, _) if otherTypes.isEmpty => False
    case CTRelationship(otherTypes, _) => otherTypes subsetOf types
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  final override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTRelationship(otherTypes, qgnOpt) =>
      if (types.isEmpty || otherTypes.isEmpty)
        CTRelationship(Set.empty[String], if (graph == qgnOpt) graph else None)
      else
        CTRelationship(types union otherTypes, if (graph == qgnOpt) graph else None)
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  final override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTRelationship(otherTypes, qgnOpt) =>
      if (types.isEmpty) other
      else if (otherTypes.isEmpty) self
      else {
        val sharedTypes = types intersect otherTypes
        if (sharedTypes.isEmpty) CTVoid else CTRelationship(sharedTypes, if (graph == qgnOpt) graph else None)
      }

    case _ =>
      super.meetMaterially(other)
  }

  override def withGraph(qgn: QualifiedGraphName): CypherType = copy(graph = Some(qgn))
}

object CTRelationshipOrNull extends CTRelationshipOrNull(Set.empty, None) with Serializable {
  def apply(types: String*): CTRelationshipOrNull =
    CTRelationshipOrNull(types.toSet)

  def apply(types: Set[String]): CTRelationshipOrNull =
    CTRelationshipOrNull(types, None)
}

sealed case class CTRelationshipOrNull(
  types: Set[String],
  override val graph: Option[QualifiedGraphName]
) extends NullableDefiniteCypherType {
  final override def name = s"$material?"

  final override def material: CTRelationship =
    CTRelationship(types, graph)
}

case object CTPath extends MaterialDefiniteCypherLeafType {
  override def name = "PATH"
}

final case class CTList(elementType: CypherType) extends MaterialDefiniteCypherType {

  self =>

  override def graph: Option[QualifiedGraphName] = elementType.graph

  override def withGraph(qgn: QualifiedGraphName): CypherType = copy(elementType = elementType.withGraph(qgn))

  override def name = s"LIST($elementType)"

  override def nullable =
    CTListOrNull(elementType)

  override def containsNullable: Boolean = elementType.containsNullable

  override def containsWildcard: Boolean = elementType.containsWildcard

  override def wildcardErasedSuperType =
    CTList(elementType.wildcardErasedSuperType)

  override def wildcardErasedSubType =
    CTList(elementType.wildcardErasedSubType)

  override def superTypeOf(other: CypherType): Ternary = other match {
    case CTList(otherEltType) => elementType superTypeOf otherEltType
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTList(otherEltType) => CTList(elementType join otherEltType)
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTList(otherEltType) => CTList(elementType meet otherEltType)
    case _ => super.meetMaterially(other)
  }
}

final case class CTListOrNull(eltType: CypherType) extends NullableDefiniteCypherType {
  override def name = s"LIST($eltType)?"

  override def material =
    CTList(eltType)

  override def containsWildcard: Boolean = eltType.containsWildcard

  override def wildcardErasedSuperType =
    CTListOrNull(eltType.wildcardErasedSuperType)

  override def wildcardErasedSubType =
    CTListOrNull(eltType.wildcardErasedSubType)
}

case object CTVoid extends MaterialDefiniteCypherType {

  self =>

  override def name = "VOID"

  override def nullable: CTNull.type = CTNull

  override def isInhabited: Ternary = False

  override def superTypeOf(other: CypherType): Ternary = other match {
    case _ if self == other => True
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = self
}

case object CTNull extends NullableDefiniteCypherType {
  override def name = "NULL"

  override def material: CTVoid.type = CTVoid
}

case object CTWildcard extends MaterialCypherType with WildcardCypherType {
  self =>

  override def name = "?"

  override def material: CTWildcard.type = self

  override def isInhabited: Ternary = Maybe

  override def sameTypeAs(other: CypherType): Ternary =
    if (!other.isNullable) Maybe else False

  override def wildcardErasedSuperType: CTAny.type = CTAny

  override def wildcardErasedSubType: CTVoid.type = CTVoid

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTAny => CTAny
    case _ => CTWildcard
  }

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTVoid => CTVoid
    case _ => CTWildcard
  }

  override def superTypeOf(other: CypherType): Ternary = other match {
    case CTVoid => True
    case _ => if (!other.isNullable) Maybe else False
  }

  override object nullable extends NullableCypherType with WildcardCypherType with Serializable {
    self =>

    override def name = "??"

    override def nullable: CTWildcard.nullable.type = self

    override def material: CTWildcard.type = CTWildcard

    override def wildcardErasedSuperType: NullableDefiniteCypherType = CTAny.nullable

    override def wildcardErasedSubType: CTNull.type = CTNull

    override def sameTypeAs(other: CypherType): Ternary =
      if (other.isNullable) Maybe else False

    override def superTypeOf(other: CypherType): Ternary = Maybe
  }

}

sealed trait CypherType extends Serializable {
  self =>

  // We distinguish types in a 4x4 matrix
  //
  // (I) nullable (includes null) vs material
  //

  // true, if null is a value of this type
  def isNullable: Boolean

  def graph: Option[QualifiedGraphName] = None

  def withGraph(qgn: QualifiedGraphName): CypherType = this

  // true, if this type only (i.e. excluding type parameters) is a wildcard (= standing for an arbitrary unknown type)
  def isWildcard: Boolean

  def isInhabited: Ternary = True

  final override def toString: String = name

  def name: String

  // identical type that additionally includes null
  def nullable: NullableCypherType

  // identical type that additionally does not include null
  def material: MaterialCypherType

  // returns this type with the same 'nullability' (i.e. either material or nullable) as typ
  final def asNullableAs(typ: CypherType): CypherType =
    if (typ.isNullable) nullable else material

  // true, if this type or any of its type parameters include null
  def containsNullable: Boolean = isNullable

  // true, if this type or any of its type parameters is a wildcard
  def containsWildcard: Boolean = isWildcard

  // smallest super type of this type that does not contain a wildcard
  def wildcardErasedSuperType: CypherType with DefiniteCypherType

  // largest sub type of this type that does not contain a wildcard
  def wildcardErasedSubType: CypherType with DefiniteCypherType

  /** join == union type == smallest shared super type */
  final def join(other: CypherType): CypherType = {
    val joined = self.material joinMaterially other.material
    if (self.isNullable || other.isNullable) joined.nullable else joined
  }

  /** meet == intersection type == largest shared sub type */
  final def meet(other: CypherType): CypherType = {
    val met = self.material meetMaterially other.material
    if (self.isNullable && other.isNullable) met.nullable else met
  }

  final def alwaysSameTypeAs(other: CypherType): Boolean = superTypeOf(other).isTrue

  final def couldBeSameTypeAs(other: CypherType): Boolean = {
    self.superTypeOf(other).maybeTrue || self.subTypeOf(other).maybeTrue
  }

  def sameTypeAs(other: CypherType): Ternary =
    if (other.isWildcard)
    // wildcard types override sameTypeAs
      other sameTypeAs self
    else
    // we rely on final case class equality for different instances of the same type
      self == other

  final def subTypeOf(other: CypherType): Ternary =
    other superTypeOf self

  /**
    * A type U is a super type of a type V iff it holds
    * that any value of type V is also a value of type U
    *
    * @return true if this type is a super type of other
    */
  def superTypeOf(other: CypherType): Ternary
}

sealed trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final override def isNullable = false

  def joinMaterially(other: MaterialCypherType): MaterialCypherType

  def meetMaterially(other: MaterialCypherType): MaterialCypherType =
    if (self superTypeOf other isTrue) other
    else if (other superTypeOf self isTrue) self
    else CTVoid

  override def wildcardErasedSuperType: MaterialCypherType with DefiniteCypherType

  override def wildcardErasedSubType: MaterialCypherType with DefiniteCypherType
}

sealed trait NullableCypherType extends CypherType {
  self =>

  final override def isNullable = true

  override def wildcardErasedSuperType: NullableCypherType with DefiniteCypherType

  override def wildcardErasedSubType: NullableCypherType with DefiniteCypherType

  override def superTypeOf(other: CypherType): Ternary =
    material superTypeOf other.material
}

sealed trait DefiniteCypherType {
  self: CypherType =>

  final override def isWildcard = false

  override def nullable: NullableCypherType with DefiniteCypherType

  override def material: MaterialCypherType with DefiniteCypherType

  override def wildcardErasedSuperType: CypherType with DefiniteCypherType

  override def wildcardErasedSubType: CypherType with DefiniteCypherType
}

sealed trait WildcardCypherType {
  self: CypherType =>

  final override def isWildcard = true

  override def nullable: NullableCypherType with WildcardCypherType

  override def material: MaterialCypherType with WildcardCypherType
}

private[okapi] object MaterialDefiniteCypherType {

  sealed private[okapi] trait DefaultOrNull {
    self: MaterialDefiniteCypherType =>

    override val nullable: NullableDefiniteCypherType = self match {
      // TODO: figure out why the previous anonymous class impl here sometimes didn't work
      // it didn't return the singleton on .material, and in some cases was not
      // equal to another instance of itself
      case CTString => CTStringOrNull
      case CTInteger => CTIntegerOrNull
      case CTBoolean => CTBooleanOrNull
      case CTAny => CTAnyOrNull
      case CTNumber => CTNumberOrNull
      case CTFloat => CTFloatOrNull
      case CTMap(inner) => CTMapOrNull(inner)
      case CTPath => CTPathOrNull
    }
  }

}

case object CTIntegerOrNull extends NullableDefiniteCypherType {
  override def name: String = CTInteger + "?"

  override def material: CTInteger.type = CTInteger
}

case object CTStringOrNull extends NullableDefiniteCypherType {
  override def name: String = CTString + "?"

  override def material: CTString.type = CTString
}

case object CTBooleanOrNull extends NullableDefiniteCypherType {
  override def name: String = CTBoolean + "?"

  override def material: CTBoolean.type = CTBoolean
}

case object CTAnyOrNull extends NullableDefiniteCypherType {
  override def name: String = CTAny + "?"

  override def material: CTAny.type = CTAny
}

case object CTNumberOrNull extends NullableDefiniteCypherType {
  override def name: String = CTNumber + "?"

  override def material: CTNumber.type = CTNumber
}

case object CTFloatOrNull extends NullableDefiniteCypherType {
  override def name: String = CTFloat + "?"

  override def material: CTFloat.type = CTFloat
}

case class CTMapOrNull(innerTypes: Map[String, CypherType]) extends NullableDefiniteCypherType {
  override def name: String = CTMap(innerTypes) + "?"

  override def material: CTMap = CTMap(innerTypes)
}

case object CTPathOrNull extends NullableDefiniteCypherType {
  override def name: String = CTPath + "?"

  override def material: CTPath.type = CTPath
}

sealed private[okapi] trait MaterialDefiniteCypherType extends MaterialCypherType with DefiniteCypherType {
  self =>

  override def material: MaterialDefiniteCypherType = self

  override def wildcardErasedSuperType: MaterialCypherType with DefiniteCypherType = self

  override def wildcardErasedSubType: MaterialCypherType with DefiniteCypherType = self
}

sealed private[okapi] trait NullableDefiniteCypherType extends NullableCypherType with DefiniteCypherType {
  self =>

  override def nullable: NullableDefiniteCypherType = self

  override def wildcardErasedSuperType: NullableCypherType with DefiniteCypherType =
    material.wildcardErasedSuperType.nullable

  override def wildcardErasedSubType: NullableCypherType with DefiniteCypherType =
    material.wildcardErasedSubType.nullable
}

sealed private[okapi] trait MaterialDefiniteCypherLeafType
  extends MaterialDefiniteCypherType
    with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def superTypeOf(other: CypherType): Ternary = other match {
    case _ if self == other => True
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case _ if self == other => self
    case CTWildcard => CTWildcard
    case CTVoid => self
    case _ => CTAny
  }
}
