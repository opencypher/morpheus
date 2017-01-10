package org.opencypher.spark.api

import org.opencypher.spark.api.types._

import scala.language.postfixOps

object types {

  case object CTAny extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    override def name = "ANY"
    override def superTypeOf(other: CypherType): Ternary = other.isMaterial
  }

  case object CTBoolean extends MaterialDefiniteCypherLeafType {
    override def name = "BOOLEAN"
  }

  case object CTNumber extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    override def name = "NUMBER"

    override def superTypeOf(other: CypherType) = other match {
      case CTNumber   => True
      case CTInteger  => True
      case CTFloat    => True
      case CTWildcard => Maybe
      case CTVoid     => True
      case _          => False
    }
  }

  case object CTInteger extends MaterialDefiniteCypherLeafType {
    override def name = "INTEGER"
  }

  case object CTFloat extends MaterialDefiniteCypherLeafType {
    override def name = "FLOAT"
  }

  case object CTString extends MaterialDefiniteCypherLeafType {
    override def name = "STRING"
  }

  case object CTMap extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    override def name = "MAP"

    override def superTypeOf(other: CypherType) = other match {
      case CTMap             => True
      case _: CTNode         => True
      case _: CTRelationship => True
      case CTWildcard        => Maybe
      case CTVoid            => True
      case _                 => False
    }
  }

  object CTNode extends CTNode(Set.empty) with Serializable {
    def apply(labels: String*): CTNode =
      if (labels.isEmpty) this else CTNode(labels.toSet)
  }

  sealed case class CTNode(labels: Set[String]) extends MaterialDefiniteCypherType {
    override def name =
      if (labels.isEmpty) "NODE" else s"${labels.map(t => s"$t").mkString(":", ":", "")} NODE"

    override def nullable =
      if (labels.isEmpty) CTNodeOrNull else CTNodeOrNull(labels)

    override def superTypeOf(other: CypherType) = other match {
      case CTNode(_) if labels.isEmpty                => True
      case CTNode(otherLabels) if otherLabels.isEmpty => False
      case CTNode(otherLabels)                        => labels subsetOf otherLabels
      case CTWildcard                                 => Maybe
      case CTVoid                                     => True
      case _                                          => False
    }
  }

  object CTNodeOrNull extends CTNodeOrNull(Set.empty) with Serializable {
    def apply(labels: String*): CTNodeOrNull =
      if (labels.isEmpty) this else CTNodeOrNull(labels.toSet)
  }

  sealed case class CTNodeOrNull(labels: Set[String]) extends NullableDefiniteCypherType {
    override def name = s"$material?"

    override def material =
      if (labels.isEmpty) CTNode else CTNode(labels)
  }

  object CTRelationship extends CTRelationship(Set.empty) with Serializable {
    def apply(types: String*): CTRelationship =
      if (types.isEmpty) this else CTRelationship(types.toSet)
  }

  sealed case class CTRelationship(types: Set[String]) extends MaterialDefiniteCypherType {
    override def name =
      if (types.isEmpty) "RELATIONSHIP" else s"${types.map(t => s"$t").mkString(":", "|", "")} RELATIONSHIP"

    override def nullable =
      if (types.isEmpty) CTRelationshipOrNull else CTRelationshipOrNull(types)

    override def superTypeOf(other: CypherType) = other match {
      case CTRelationship(_) if types.isEmpty               => True
      case CTRelationship(otherTypes) if otherTypes.isEmpty => False
      case CTRelationship(otherTypes)                       => otherTypes subsetOf types
      case CTWildcard                                       => Maybe
      case CTVoid                                           => True
      case _                                                => False
    }
  }

  object CTRelationshipOrNull extends CTRelationshipOrNull(Set.empty) with Serializable {
    def apply(types: String*): CTRelationshipOrNull =
      if (types.isEmpty) this else CTRelationshipOrNull(types.toSet)
  }

  sealed case class CTRelationshipOrNull(types: Set[String]) extends NullableDefiniteCypherType {
    override def name = s"$material?"

    override def material =
      if (types.isEmpty) CTRelationship else CTRelationship(types)
  }

  case object CTPath extends MaterialDefiniteCypherLeafType {
    override def name = "PATH"
  }

  final case class CTList(eltType: CypherType) extends MaterialDefiniteCypherType {
    override def name = s"LIST OF $eltType"

    override def nullable =
      CTListOrNull(eltType)

    override def containsNullable = eltType.containsNullable
    override def containsWildcard = eltType.containsWildcard

    override def erasedSuperType =
      CTList(eltType.erasedSuperType)

    override def erasedSubType =
      CTList(eltType.erasedSubType)

    override def superTypeOf(other: CypherType) = other match {
      case CTList(otherEltType) => eltType superTypeOf otherEltType
      case CTWildcard           => Maybe
      case CTVoid               => True
      case _                    => False
    }
  }

  final case class CTListOrNull(eltType: CypherType) extends NullableDefiniteCypherType {
    override def name = s"LIST? OF $eltType"

    override def material =
      CTList(eltType)

    override def containsWildcard = eltType.containsWildcard

    override def erasedSuperType =
      CTListOrNull(eltType.erasedSuperType)

    override def erasedSubType =
      CTListOrNull(eltType.erasedSubType)
  }

  case object CTVoid extends MaterialDefiniteCypherType {
    self =>

    override def name = "VOID"

    override def nullable = CTNull

    override def isInhabited: Ternary = False

    override def superTypeOf(other: CypherType) = other match {
      case _ if self == other => True
      case CTWildcard         => Maybe
      case CTVoid             => True
      case _                  => False
    }
  }

  case object CTNull extends NullableDefiniteCypherType {
    override def name = "NULL"

    override def material = CTVoid
  }

  case object CTWildcard extends MaterialCypherType with WildcardCypherType {
    self =>

    override def name = "?"

    override def material = self

    override def isInhabited: Ternary = Maybe

    override def sameTypeAs(other: CypherType) =
      if (other.isMaterial) Maybe else False

    override def erasedSuperType = CTAny
    override def erasedSubType = CTVoid

    // super type of
    override def superTypeOf(other: CypherType): Ternary = other match {
      case CTVoid => True
      case _      => if (other.isMaterial) Maybe else False
    }

    override object nullable extends NullableCypherType with WildcardCypherType with Serializable {
      self =>

      override def name = "??"

      override def nullable = self
      override def material = CTWildcard

      override def erasedSuperType = CTAny.nullable
      override def erasedSubType = CTNull

      override def sameTypeAs(other: CypherType) =
        if (other.isNullable) Maybe else False

      override def superTypeOf(other: CypherType) = Maybe
    }
  }
}


object CypherType {

  // Values in the same order group are ordered (sorted) together
  type OrderGroup = OrderGroups.Value

  object OrderGroups extends Enumeration with Serializable {
    val MapOrderGroup = Value("MAP ORDER GROUP")
    val NodeOrderGroup = Value("NODE ORDER GROUP")
    val RelationshipOrderGroup = Value("RELATIONSHIP ORDER GROUP")
    val PathOrderGroup = Value("PATH ORDER GROUP")
    val ListOrderGroup = Value("LIST ORDER GROUP")
    val StringOrderGroup = Value("STRING ORDER GROUP")
    val BooleanOrderGroup = Value("BOOLEAN ORDER GROUP")
    val NumberOrderGroup = Value("NUMBER ODER GROUP")
    val VoidOrderGroup = Value("VOID ODER GROUP")
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

  // false, if null is a value of this type
  def isMaterial: Boolean

  // (II) definite (a single known type) vs a wildcard (standing for an arbitrary unknown type)
  //

  // true, if this type only (i.e. excluding type parameters) is not a wildcard
  def isDefinite: Boolean

  // true, if this type only (i.e. excluding type parameters) is a wildcard
  def isWildcard: Boolean

  def isInhabited: Ternary = True

  final override def toString: String = name

  def name: String

  // identical type that additionally includes null
  def nullable: NullableCypherType

  // identical type that additionally does not include null
  def material: MaterialCypherType

  // returns this type with the same 'nullability' (i.e. either material or nullable) as typ
  def asNullableAs(typ: CypherType) =
    if (typ.isNullable) nullable else material

  // true, if this type or any of its type parameters include null
  def containsNullable = isNullable

  // true, if this type or any of its type parameters is a wildcard
  def containsWildcard = isWildcard

  // smallest super type of this type that does not contain a wildcard
  def erasedSuperType: CypherType with DefiniteCypherType

  // largest sub type of this type that does not contain a wildcard
  def erasedSubType: CypherType with DefiniteCypherType

  // join == union type == smallest shared super type
  final def join(other: CypherType): CypherType =
    if (self superTypeOf other isTrue) self
    else if (other superTypeOf self isTrue) other
    else if (self.isMaterial && other.isMaterial) CTAny
    else CTAny.nullable

  // meet == intersection type == largest shared sub type
  final def meet(other: CypherType): CypherType =
    if (self subTypeOf other isTrue) self
    else if (other subTypeOf self isTrue) other
    else if (self.isNullable && other.isNullable) CTNull
    else CTVoid

  def sameTypeAs(other: CypherType): Ternary =
    if (other.isWildcard)
      // wildcard types override sameTypeAs
      other sameTypeAs self
    else
      // we rely on final case class equality for different instances of the same type
      self == other

  final def subTypeOf(other: CypherType): Ternary =
    other superTypeOf self

  def superTypeOf(other: CypherType): Ternary
}

sealed trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final override def isNullable = false
  final override def isMaterial = true

  override def erasedSuperType: MaterialCypherType with DefiniteCypherType
  override def erasedSubType: MaterialCypherType with DefiniteCypherType
}

sealed trait NullableCypherType extends CypherType {
  self =>

  final override def isNullable = true
  final override def isMaterial = false

  override def erasedSuperType: NullableCypherType with DefiniteCypherType
  override def erasedSubType: NullableCypherType with DefiniteCypherType

  override def superTypeOf(other: CypherType) =
    material superTypeOf other.material
}

sealed trait DefiniteCypherType {
  self: CypherType =>

  final override def isDefinite = true
  final override def isWildcard = false

  override def nullable: NullableCypherType with DefiniteCypherType
  override def material: MaterialCypherType with DefiniteCypherType

  override def erasedSuperType: CypherType with DefiniteCypherType
  override def erasedSubType: CypherType with DefiniteCypherType
}

sealed trait WildcardCypherType {
  self: CypherType =>

  final override def isDefinite = false
  final override def isWildcard = true

  override def nullable: NullableCypherType with WildcardCypherType
  override def material: MaterialCypherType with WildcardCypherType
}

private[spark] object MaterialDefiniteCypherType {
  sealed private[spark] trait DefaultOrNull {
    self: MaterialDefiniteCypherType =>

    override val nullable = new NullableDefiniteCypherType {
      override def material = self
      override def name = self + "?"
    }
  }
}

sealed private[spark] trait MaterialDefiniteCypherType extends MaterialCypherType with DefiniteCypherType {
  self =>

  override def material = self

  override def erasedSuperType: MaterialCypherType with DefiniteCypherType = self
  override def erasedSubType: MaterialCypherType with DefiniteCypherType= self
}

sealed private[spark] trait NullableDefiniteCypherType extends NullableCypherType with DefiniteCypherType {
  self =>

  override def nullable = self

  override def erasedSuperType: NullableCypherType with DefiniteCypherType = material.erasedSuperType.nullable
  override def erasedSubType: NullableCypherType with DefiniteCypherType = material.erasedSubType.nullable
}

sealed private[spark] trait MaterialDefiniteCypherLeafType
  extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def superTypeOf(other: CypherType) = other match {
    case _ if self == other => True
    case CTWildcard         => Maybe
    case CTVoid             => True
    case _                  => False
  }
}
