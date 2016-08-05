package org.opencypher.spark

import CypherTypes._
import Ternary.implicits._

object CypherTypes {

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
      case CTNumber  => true
      case CTInteger => true
      case CTFloat   => true
      case CTWildcard => Maybe
      case CTVoid    => true
      case _         => false
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
      case CTMap          => true
      case CTNode         => true
      case CTRelationship => true
      case CTWildcard     => Maybe
      case CTVoid         => true
      case _              => false
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

    override def definiteSuperType =
      CTList(eltType.definiteSuperType)

    override def definiteSubType =
      CTList(eltType.definiteSubType)

    def superTypeOf(other: CypherType) = other match {
      case CTList(otherEltType) => eltType superTypeOf otherEltType
      case CTWildcard           => Maybe
      case CTVoid               => true
      case _                    => false
    }
  }

  case class CTListOrNull(eltType: CypherType) extends NullableDefiniteCypherType {
    def name = s"LIST? OF $eltType"

    def material =
      CTList(eltType)

    override def definiteSuperType =
      CTListOrNull(eltType.definiteSuperType)

    override def definiteSubType =
      CTListOrNull(eltType.definiteSubType)
  }

  case object CTVoid extends MaterialDefiniteCypherType {
    self =>

    def name = "VOID"

    def nullable = CTNull

    override def superTypeOf(other: CypherType) = other match {
      case _ if self == other => true
      case CTWildcard         => Maybe
      case CTVoid             => true
      case _                  => false
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

    def definiteSuperType = CTAny
    def definiteSubType = CTVoid

    // super type of
    override def superTypeOf(other: CypherType): Ternary = other match {
      case CTVoid => true
      case _      => if (other.isMaterial) Maybe else False
    }

    object nullable extends NullableCypherType with WildcardCypherType {
      self =>

      def name = "??"

      def nullable = self
      def material = CTWildcard

      override def definiteSuperType = CTAny.nullable
      override def definiteSubType = CTNull

      override def sameTypeAs(other: CypherType) =
        if (other.isNullable) Maybe else False

      override def superTypeOf(other: CypherType) = Maybe
    }
  }
}

sealed trait CypherType {
  self =>

  def isNullable: Boolean
  def isMaterial: Boolean

  def isDefinite: Boolean
  def isWildcard: Boolean

  final override def toString: String = name

  def name: String

  def nullable: NullableCypherType
  def material: MaterialCypherType

  def definiteSuperType: CypherType with DefiniteCypherType
  def definiteSubType: CypherType with DefiniteCypherType

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

  override def definiteSuperType: MaterialCypherType with DefiniteCypherType
  override def definiteSubType: MaterialCypherType with DefiniteCypherType
}

sealed trait NullableCypherType extends CypherType {
  self =>

  final def isNullable = true
  final def isMaterial = false

  override def definiteSuperType: NullableCypherType with DefiniteCypherType
  override def definiteSubType: NullableCypherType with DefiniteCypherType

  def superTypeOf(other: CypherType) =
    material superTypeOf other.material
}

sealed trait DefiniteCypherType {
  self: CypherType =>

  final def isDefinite = true
  final def isWildcard = false

  override def nullable: NullableCypherType with DefiniteCypherType
  override def material: MaterialCypherType with DefiniteCypherType

  override def definiteSuperType: CypherType with DefiniteCypherType
  override def definiteSubType: CypherType with DefiniteCypherType
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

  override def definiteSuperType: MaterialCypherType with DefiniteCypherType = self
  override def definiteSubType: MaterialCypherType with DefiniteCypherType= self
}

sealed private[spark] trait NullableDefiniteCypherType extends NullableCypherType with DefiniteCypherType {
  self =>

  override def nullable = self

  def definiteSuperType: NullableCypherType with DefiniteCypherType = material.definiteSuperType.nullable
  def definiteSubType: NullableCypherType with DefiniteCypherType = material.definiteSubType.nullable
}

sealed private[spark] trait MaterialDefiniteCypherLeafType extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
  self =>

  def superTypeOf(other: CypherType) = other match {
    case _ if self == other => true
    case CTWildcard         => Maybe
    case CTVoid             => true
    case _                  => false
  }
}
