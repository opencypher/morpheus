package org.opencypher.spark.api

import org.opencypher.spark.api.types._

import scala.language.postfixOps

import Ternary.Conversion._

object types {

  case object CTAny extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    def name = "ANY"

    def superTypeOf(other: CypherType): Ternary = other.isMaterial
  }

  case object CTBoolean extends MaterialDefiniteCypherLeafType {
    def name = "BOOLEAN"
  }

  case object CTNumber extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    def name = "NUMBER"

    final def superTypeOf(other: CypherType) = other match {
      case CTNumber   => True
      case CTInteger  => True
      case CTFloat    => True
      case CTWildcard => Maybe
      case CTVoid     => True
      case _          => False
    }
  }

  case object CTInteger extends MaterialDefiniteCypherLeafType {
    def name = "INTEGER"
  }

  case object CTFloat extends MaterialDefiniteCypherLeafType {
    def name = "FLOAT"
  }

  case object CTString extends MaterialDefiniteCypherLeafType {
    def name = "STRING"
  }

  case object CTMap extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
    def name = "MAP"

    def superTypeOf(other: CypherType) = other match {
      case CTMap          => True
      case CTNode         => True
      case CTRelationship => True
      case CTWildcard     => Maybe
      case CTVoid         => True
      case _              => False
    }
  }

  case object CTNode extends MaterialDefiniteCypherLeafType {
    def name = "NODE"
  }

  case object CTRelationship extends MaterialDefiniteCypherLeafType {
    def name = "RELATIONSHIP"
  }

  case object CTPath extends MaterialDefiniteCypherLeafType {
    def name = "PATH"
  }

  case class CTList(eltType: CypherType) extends MaterialDefiniteCypherType {
    def name =s"LIST OF $eltType"

    def nullable =
      CTListOrNull(eltType)

    override def containsNullable = eltType.containsNullable
    override def containsWildcard = eltType.containsWildcard

    override def erasedSuperType =
      CTList(eltType.erasedSuperType)

    override def erasedSubType =
      CTList(eltType.erasedSubType)

    def superTypeOf(other: CypherType) = other match {
      case CTList(otherEltType) => eltType superTypeOf otherEltType
      case CTWildcard           => Maybe
      case CTVoid               => True
      case _                    => False
    }
  }

  case class CTListOrNull(eltType: CypherType) extends NullableDefiniteCypherType {
    def name = s"LIST? OF $eltType"

    def material =
      CTList(eltType)

    override def containsWildcard = eltType.containsWildcard

    override def erasedSuperType =
      CTListOrNull(eltType.erasedSuperType)

    override def erasedSubType =
      CTListOrNull(eltType.erasedSubType)
  }

  case object CTVoid extends MaterialDefiniteCypherType {
    self =>

    def name = "VOID"

    def nullable = CTNull

    override def superTypeOf(other: CypherType) = other match {
      case _ if self == other => True
      case CTWildcard         => Maybe
      case CTVoid             => True
      case _                  => False
    }
  }

  case object CTNull extends NullableDefiniteCypherType {
    def name = "NULL"

    def material = CTVoid
  }

  case object CTWildcard extends MaterialCypherType with WildcardCypherType {
    self =>

    def name = "?"

    override def material = self

    override def sameTypeAs(other: CypherType) =
      if (other.isMaterial) Maybe else False

    def erasedSuperType = CTAny
    def erasedSubType = CTVoid

    // super type of
    override def superTypeOf(other: CypherType): Ternary = other match {
      case CTVoid => True
      case _      => if (other.isMaterial) Maybe else False
    }

    object nullable extends NullableCypherType with WildcardCypherType {
      self =>

      def name = "??"

      def nullable = self
      def material = CTWildcard

      override def erasedSuperType = CTAny.nullable
      override def erasedSubType = CTNull

      override def sameTypeAs(other: CypherType) =
        if (other.isNullable) Maybe else False

      override def superTypeOf(other: CypherType) = Maybe
    }
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

  final override def toString: String = name

  def name: String

  // identical type that additionally includes null
  def nullable: NullableCypherType

  // identical type that additionally does not include null
  def material: MaterialCypherType

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
      // we rely on case class equality for different instances of the same type
      self == other

  final def subTypeOf(other: CypherType): Ternary =
    other superTypeOf self

  def superTypeOf(other: CypherType): Ternary
}

sealed trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final def isNullable = false
  final def isMaterial = true

  override def erasedSuperType: MaterialCypherType with DefiniteCypherType
  override def erasedSubType: MaterialCypherType with DefiniteCypherType

  def asNullableAs(typ: CypherType) =
    if (typ.isNullable) nullable else material
}

sealed trait NullableCypherType extends CypherType {
  self =>

  final def isNullable = true
  final def isMaterial = false

  override def erasedSuperType: NullableCypherType with DefiniteCypherType
  override def erasedSubType: NullableCypherType with DefiniteCypherType

  def superTypeOf(other: CypherType) =
    material superTypeOf other.material
}

sealed trait DefiniteCypherType {
  self: CypherType =>

  final def isDefinite = true
  final def isWildcard = false

  override def nullable: NullableCypherType with DefiniteCypherType
  override def material: MaterialCypherType with DefiniteCypherType

  override def erasedSuperType: CypherType with DefiniteCypherType
  override def erasedSubType: CypherType with DefiniteCypherType
}

sealed trait WildcardCypherType {
  self: CypherType =>

  final def isDefinite = false
  final def isWildcard = true

  override def nullable: NullableCypherType with WildcardCypherType
  override def material: MaterialCypherType with WildcardCypherType
}

private[spark] object MaterialDefiniteCypherType {
  sealed private[spark] trait DefaultOrNull {
    self: MaterialDefiniteCypherType =>

    val nullable = new NullableDefiniteCypherType {
      def material = self

      def name = self + "?"
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

  def erasedSuperType: NullableCypherType with DefiniteCypherType = material.erasedSuperType.nullable
  def erasedSubType: NullableCypherType with DefiniteCypherType = material.erasedSubType.nullable
}

sealed private[spark] trait MaterialDefiniteCypherLeafType extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
  self =>

  def superTypeOf(other: CypherType) = other match {
    case _ if self == other => True
    case CTWildcard         => Maybe
    case CTVoid             => True
    case _                  => False
  }
}
