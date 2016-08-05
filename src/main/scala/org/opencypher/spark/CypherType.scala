package org.opencypher.spark

import CypherTypes._

object CypherTypes {

  case object CTAny extends MaterialCypherType with SingleCypherType with DefaultOrNull {
    protected def materialName = "ANY"

    def superTypeOf(other: CypherType) = other.isMaterialType
  }

  case object CTBoolean extends SingleMaterialCypherLeafType {
    protected def materialName = "BOOLEAN"
  }

  case object CTNumber extends MaterialCypherType with SingleCypherType with DefaultOrNull {
    protected def materialName = "NUMBER"

    final def superTypeOf(other: CypherType) = other match {
      case CTNumber  => true
      case CTInteger => true
      case CTFloat   => true
      case CTVoid    => true
      case _         => false
    }
  }

  case object CTInteger extends SingleMaterialCypherLeafType {
    protected def materialName = "INTEGER"
  }

  case object CTFloat extends SingleMaterialCypherLeafType {
    protected def materialName = "FLOAT"
  }

  case object CTString extends SingleMaterialCypherLeafType {
    protected def materialName = "STRING"
  }

  case object CTMap extends MaterialCypherType with SingleCypherType with DefaultOrNull {
    protected def materialName = "MAP"

    def superTypeOf(other: CypherType) = other match {
      case CTMap          => true
      case CTNode         => true
      case CTRelationship => true
      case CTVoid         => true
      case _              => false
    }
  }

  case object CTNode extends SingleMaterialCypherLeafType {
    protected def materialName = "NODE"
  }

  case object CTRelationship extends SingleMaterialCypherLeafType {
    protected def materialName = "RELATIONSHIP"
  }

  case object CTPath extends SingleMaterialCypherLeafType {
    protected def materialName = "PATH"
  }

  case class CTList(eltType: CypherType) extends MaterialCypherType with SingleCypherType with DefaultOrNull {
    protected def materialName = s"LIST OF $eltType"
    override protected def nullableName = s"LIST? OF $eltType"

    def superTypeOf(other: CypherType) = other match {
      case CTList(otherEltType) => eltType superTypeOf otherEltType
      case CTVoid               => true
      case _                    => false
    }
  }

  def CTNull = CTVoid.orNull

  case object CTVoid extends SingleMaterialCypherLeafType {
    def materialName = "VOID"
    override def nullableName = "NULL"
  }

  case object CTUnknown extends MaterialCypherType with WildcardCypherType {
    self =>

    protected def materialName: String = "?"

    // super type of
    override def superTypeOf(other: CypherType): Boolean = ???

    object orNull extends NullableCypherType with WildcardCypherType {
      def withoutNull = self

      final override def toString = nullableName

      def superTypeOf(other: CypherType) =
        withoutNull superTypeOf other.withoutNull
    }
  }
}

sealed trait CypherType {
  self =>

  def isNullableType: Boolean
  def isMaterialType: Boolean

  def isSingleType: Boolean
  def isWildcardType: Boolean

  def orNull: CypherType
  def withoutNull: CypherType

  // join == union type == smallest shared super type
  final def join(other: CypherType): CypherType =
    if (self superTypeOf other) self
    else if (other superTypeOf self) other
    else if (self.isMaterialType && other.isMaterialType) CTAny
    else CTAny.orNull

  // meet == intersection type == largest shared sub type
  final def meet(other: CypherType): CypherType =
    if (self subTypeOf other) self
    else if (other subTypeOf self) other
    else if (self.isNullableType || other.isNullableType) CTVoid.orNull
    else CTVoid

  // subtype of
  final def subTypeOf(other: CypherType): Boolean =
    other superTypeOf self

  // super type of
  def superTypeOf(other: CypherType): Boolean
}

sealed private[spark] trait SingleCypherType extends CypherType {
  final def isSingleType = true
  final def isWildcardType = false
}

sealed private[spark] trait WildcardCypherType extends CypherType {
  final def isSingleType = false
  final def isWildcardType = true
}

sealed private[spark] trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final def isNullableType = false
  final def isMaterialType = true

  final def withoutNull = self

  final override def toString = materialName

  protected def materialName: String
  protected def nullableName: String = s"$materialName?"
}

sealed private[spark] trait DefaultOrNull {
  self: MaterialCypherType =>

  object orNull extends NullableCypherType with SingleCypherType {
    def withoutNull = self

    final override def toString = nullableName

    def superTypeOf(other: CypherType) =
      withoutNull superTypeOf other.withoutNull
  }
}

sealed private[spark] trait NullableCypherType extends CypherType {
  self =>

  final def isNullableType = true
  final def isMaterialType = false

  final def orNull = self
}

sealed private[spark] trait SingleMaterialCypherLeafType extends MaterialCypherType with SingleCypherType with DefaultOrNull {
  self =>

  def superTypeOf(other: CypherType) = other match {
    case _ if self == other => true
    case CTVoid             => true
    case _                  => false
  }
}
