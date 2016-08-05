package org.opencypher.spark

import CypherTypes._

object CypherTypes {

  case object CTAny extends MaterialCypherType {
    def materialName = "ANY"

    def superTypeOf(other: CypherType) = other.isMaterial
  }

  case object CTBoolean extends MaterialCypherLeafType {
    def materialName = "BOOLEAN"
  }

  case object CTNumber extends MaterialCypherType {
    def materialName = "NUMBER"

    final def superTypeOf(other: CypherType) = other match {
      case CTNumber  => true
      case CTInteger => true
      case CTFloat   => true
      case CTVoid    => true
      case _         => false
    }
  }

  case object CTInteger extends MaterialCypherLeafType {
    def materialName = "INTEGER"
  }

  case object CTFloat extends MaterialCypherLeafType {
    def materialName = "FLOAT"
  }

  case object CTString extends MaterialCypherLeafType {
    def materialName = "STRING"
  }

  case object CTMap extends MaterialCypherType {
    def materialName = "MAP"

    def superTypeOf(other: CypherType) = other match {
      case CTMap          => true
      case CTNode         => true
      case CTRelationship => true
      case CTVoid         => true
      case _              => false
    }
  }

  case object CTNode extends MaterialCypherLeafType {
    def materialName = "NODE"
  }

  case object CTRelationship extends MaterialCypherLeafType {
    def materialName = "RELATIONSHIP"
  }

  case object CTPath extends MaterialCypherLeafType {
    def materialName = "PATH"
  }

  case class CTList(eltType: CypherType) extends MaterialCypherType {
    def materialName = s"LIST OF $eltType"
    override def nullableName = s"LIST? OF $eltType"

    def superTypeOf(other: CypherType) = other match {
      case CTList(otherEltType) => eltType superTypeOf otherEltType
      case CTVoid               => true
      case _                    => false
    }
  }

  def CTNull = CTVoid.orNull

  case object CTVoid extends MaterialCypherLeafType {
    def materialName = "VOID"
    override def nullableName = "NULL"
  }
}

sealed trait CypherType {
  self =>

  def isNullable: Boolean
  def isMaterial: Boolean

  def orNull: CypherType
  def withoutNull: CypherType

  // join == union type == smallest shared super type
  final def join(other: CypherType): CypherType =
    if (self superTypeOf other) self
    else if (other superTypeOf self) other
    else if (self.isMaterial && other.isMaterial) CTAny
    else CTAny.orNull

  // meet == intersection type == largest shared sub type
  final def meet(other: CypherType): CypherType =
    if (self subTypeOf other) self
    else if (other subTypeOf self) other
    else if (self.isNullable || other.isNullable) CTVoid.orNull
    else CTVoid

  // subtype of
  final def subTypeOf(other: CypherType): Boolean =
    other superTypeOf self

  // super type of
  def superTypeOf(other: CypherType): Boolean
}

sealed private[spark] trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final def isNullable = false
  final def isMaterial = true

  final def withoutNull = self

  final override def toString = materialName

  protected def materialName: String
  protected def nullableName: String = s"$materialName?"

  object orNull extends NullableCypherType {
    def withoutNull = self

    final override def toString = nullableName

    def superTypeOf(other: CypherType) =
      withoutNull superTypeOf other.withoutNull
  }
}

sealed private[spark] trait NullableCypherType extends CypherType {
  self =>

  final def isNullable = true
  final def isMaterial = false

  final def orNull = self
}

sealed private[spark] trait MaterialCypherLeafType extends MaterialCypherType {
  self =>

  def superTypeOf(other: CypherType) = other match {
    case _ if self == other => true
    case CTVoid             => true
    case _                  => false
  }
}
