package org.opencypher.spark.api.types

import cats.Monoid

import scala.language.postfixOps

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

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x join y
  }

  implicit val meetMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTAny.nullable

    override def combine(x: CypherType, y: CypherType): CypherType = x meet y
  }
}

case object CTAny extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {
  override def name = "ANY"

  override def superTypeOf(other: CypherType): Ternary = other.isMaterial

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = this

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other
}

case object CTBoolean extends MaterialDefiniteCypherLeafType {
  override def name = "BOOLEAN"
}

case object CTNumber extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def name = "NUMBER"

  override def superTypeOf(other: CypherType) = other match {
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

case object CTMap extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def name = "MAP"

  override def superTypeOf(other: CypherType) = other match {
    case CTMap => True
    case _: CTNode => True
    case _: CTRelationship => True
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTMap => self
    case _: CTNode => self
    case _: CTRelationship => self
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }
}

object CTNode extends CTNode(Map.empty) with Serializable {
  def apply(labels: String*): CTNode =
    if (labels.isEmpty) this else CTNode(labels.map(l => l -> true).toMap)
}

sealed case class CTNode(labels: Map[String, Boolean]) extends MaterialDefiniteCypherType {

  self =>

  final override def name =
    if (labels.isEmpty) "NODE" else s"${
      labels.map {
        case (l, true) => s"$l"
        case (l, false) => s"-$l"
      }.mkString(":", ":", "")
    } NODE"

  final override def nullable =
    if (labels.isEmpty) CTNodeOrNull else CTNodeOrNull(labels)

  final override def superTypeOf(other: CypherType) = other match {
    case CTNode(_) if labels.isEmpty => True
    case CTNode(otherLabels) if otherLabels.isEmpty => False
    case CTNode(otherLabels) => superLabelsOf(otherLabels)
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  final override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTMap => CTMap
    case CTNode(otherLabels) => unionLabels(otherLabels)
    case _: CTRelationship => CTMap
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  final override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTNode(otherLabels) => intersectLabels(otherLabels)
    case _ => super.meetMaterially(other)
  }

  // returns if any node that matches otherLabels is also a node that matches labels
  private def superLabelsOf(otherLabels: Map[String, Boolean]): Ternary = {
    val pieces = labels.keySet.union(otherLabels.keySet).map { key =>
      (labels.get(key), otherLabels.get(key)) match {

        // TRUE :Person >= :Person
        // TRUE :-Person >= :-Person
        case (Some(x), Some(y)) if x == y => True

        // FALSE :-Person >= :Person
        // FALSE :Person >= :-Person
        case (Some(_), Some(_)) => False

        // TRUE ?Person >= :Person
        // TRUE ?Person >= :-Person
        case (None, Some(_)) => True

        // MAYBE :Person >= ?Person
        // MAYBE :-Person >= ?Person
        case (Some(_), None) => Maybe

        // TRUE ?Person >= ?Person
        case (None, None) => True
      }
    }
    pieces.foldLeft[Ternary](True)(_ and _)
  }

  private def intersectLabels(otherLabels: Map[String, Boolean]): MaterialCypherType = {
    labels.keySet.union(otherLabels.keySet).foldLeft[Option[Map[String, Boolean]]](Some(Map.empty)) {

      // Empty => Empty
      case (None, _) => None

      case (acc@Some(m), key) =>
        (labels.get(key), otherLabels.get(key)) match {

          // :Person /\ :Person => :Person
          // :-Person /\ :-Person => :Person
          case (Some(x), Some(y)) if x == y => Some(m.updated(key, x))

          // :Person /\ :-Person => Empty
          // :-Person /\ :Person => Empty
          case (Some(_), Some(_)) => None

          // :[x]Person /\ ?Person => :[x]Person
          case (Some(x), None) => Some(m.updated(key, x))

          // :?Person /\ [x]Person => :[x]Person
          case (None, Some(y)) => Some(m.updated(key, y))

          // ?Person /\ ?Person => ?Person
          case _ => acc
        }
    }.map(CTNode.apply).getOrElse(CTVoid)
  }

  private def unionLabels(otherLabels: Map[String, Boolean]): MaterialCypherType = {
    val newLabels = labels.keySet.union(otherLabels.keySet).foldLeft[Map[String, Boolean]](Map.empty) {
      case (m, key) =>
        (labels.get(key), otherLabels.get(key)) match {
          // :Person \/ :Person => :Person
          // :-Person \/ :-Person => :Person
          case (Some(x), Some(y)) if x == y => m.updated(key, x)

          // :Person \/ :-Person => ?Person
          // :-Person \/ :Person => ?Person
          // :[x]Person \/ ?Person => ?Person
          // :?Person \/ [x]Person => ?Person
          // ?Person \/ ?Person => ?Person
          case _ => m
        }
    }
    CTNode(newLabels)
  }
}

object CTNodeOrNull extends CTNodeOrNull(Map.empty) with Serializable {
  def apply(labels: String*): CTNodeOrNull =
    if (labels.isEmpty) this else CTNodeOrNull(labels.map(l => l -> true).toMap)
}

sealed case class CTNodeOrNull(labels: Map[String, Boolean]) extends NullableDefiniteCypherType {
  final override def name = s"$material?"

  final override def material =
    if (labels.isEmpty) CTNode else CTNode(labels)
}

object CTRelationship extends CTRelationship(Set.empty) with Serializable {
  def apply(types: String*): CTRelationship =
    if (types.isEmpty) this else CTRelationship(types.toSet)
}

sealed case class CTRelationship(types: Set[String]) extends MaterialDefiniteCypherType {

  self =>

  final override def name =
    if (types.isEmpty) "RELATIONSHIP" else s"${types.map(t => s"$t").mkString(":", "|", "")} RELATIONSHIP"

  final override def nullable =
    if (types.isEmpty) CTRelationshipOrNull else CTRelationshipOrNull(types)

  final override def superTypeOf(other: CypherType) = other match {
    case CTRelationship(_) if types.isEmpty => True
    case CTRelationship(otherTypes) if otherTypes.isEmpty => False
    case CTRelationship(otherTypes) => otherTypes subsetOf types
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  final override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTMap => CTMap
    case CTRelationship(otherTypes) =>
      if (types.isEmpty || otherTypes.isEmpty) CTRelationship else CTRelationship(types union otherTypes)
    case _: CTNode => CTMap
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  final override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTRelationship(otherTypes) =>
      if (types.isEmpty) other
      else if (otherTypes.isEmpty) self
      else {
        val sharedTypes = types intersect otherTypes
        if (sharedTypes.isEmpty) CTVoid else CTRelationship(sharedTypes)
      }

    case _ =>
      super.meetMaterially(other)
  }
}

object CTRelationshipOrNull extends CTRelationshipOrNull(Set.empty) with Serializable {
  def apply(types: String*): CTRelationshipOrNull =
    if (types.isEmpty) this else CTRelationshipOrNull(types.toSet)
}

sealed case class CTRelationshipOrNull(types: Set[String]) extends NullableDefiniteCypherType {
  final override def name = s"$material?"

  final override def material =
    if (types.isEmpty) CTRelationship else CTRelationship(types)
}

case object CTPath extends MaterialDefiniteCypherLeafType {
  override def name = "PATH"
}

final case class CTList(eltType: CypherType) extends MaterialDefiniteCypherType {

  self =>

  override def name = s"LIST OF $eltType"

  override def nullable =
    CTListOrNull(eltType)

  override def containsNullable = eltType.containsNullable

  override def containsWildcard = eltType.containsWildcard

  override def wildcardErasedSuperType =
    CTList(eltType.wildcardErasedSuperType)

  override def wildcardErasedSubType =
    CTList(eltType.wildcardErasedSubType)

  override def superTypeOf(other: CypherType) = other match {
    case CTList(otherEltType) => eltType superTypeOf otherEltType
    case CTWildcard => Maybe
    case CTVoid => True
    case _ => False
  }

  override def joinMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTList(otherEltType) => CTList(eltType join otherEltType)
    case CTVoid => self
    case CTWildcard => CTWildcard
    case _ => CTAny
  }

  override def meetMaterially(other: MaterialCypherType): MaterialCypherType = other match {
    case CTList(otherEltType) => CTList(eltType meet otherEltType)
    case _ => super.meetMaterially(other)
  }
}

final case class CTListOrNull(eltType: CypherType) extends NullableDefiniteCypherType {
  override def name = s"LIST? OF $eltType"

  override def material =
    CTList(eltType)

  override def containsWildcard = eltType.containsWildcard

  override def wildcardErasedSuperType =
    CTListOrNull(eltType.wildcardErasedSuperType)

  override def wildcardErasedSubType =
    CTListOrNull(eltType.wildcardErasedSubType)
}

case object CTVoid extends MaterialDefiniteCypherType {

  self =>

  override def name = "VOID"

  override def nullable = CTNull

  override def isInhabited: Ternary = False

  override def superTypeOf(other: CypherType) = other match {
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

  override def material = CTVoid
}

case object CTWildcard extends MaterialCypherType with WildcardCypherType {
  self =>

  override def name = "?"

  override def material = self

  override def isInhabited: Ternary = Maybe

  override def sameTypeAs(other: CypherType) =
    if (other.isMaterial) Maybe else False

  override def wildcardErasedSuperType = CTAny

  override def wildcardErasedSubType = CTVoid

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
    case _ => if (other.isMaterial) Maybe else False
  }

  override object nullable extends NullableCypherType with WildcardCypherType with Serializable {
    self =>

    override def name = "??"

    override def nullable = self

    override def material = CTWildcard

    override def wildcardErasedSuperType = CTAny.nullable

    override def wildcardErasedSubType = CTNull

    override def sameTypeAs(other: CypherType) =
      if (other.isNullable) Maybe else False

    override def superTypeOf(other: CypherType) = Maybe
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
  final def asNullableAs(typ: CypherType) =
    if (typ.isNullable) nullable else material

  // true, if this type or any of its type parameters include null
  def containsNullable = isNullable

  // true, if this type or any of its type parameters is a wildcard
  def containsWildcard = isWildcard

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

  final def alwaysSameTypeAs(other: CypherType) = superTypeOf(other).isTrue

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

  def superTypeOf(other: CypherType): Ternary
}

sealed trait MaterialCypherType extends CypherType {
  self: CypherType =>

  final override def isNullable = false

  final override def isMaterial = true

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

  final override def isMaterial = false

  override def wildcardErasedSuperType: NullableCypherType with DefiniteCypherType

  override def wildcardErasedSubType: NullableCypherType with DefiniteCypherType

  override def superTypeOf(other: CypherType) =
    material superTypeOf other.material
}

sealed trait DefiniteCypherType {
  self: CypherType =>

  final override def isDefinite = true

  final override def isWildcard = false

  override def nullable: NullableCypherType with DefiniteCypherType

  override def material: MaterialCypherType with DefiniteCypherType

  override def wildcardErasedSuperType: CypherType with DefiniteCypherType

  override def wildcardErasedSubType: CypherType with DefiniteCypherType
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

    override val nullable = self match {
      case CTString => CTStringOrNull // workaround
    // TODO: This causes some nullable types to be non-equal ?!
      case x => new NullableDefiniteCypherType {
        override def material = self

        override def name = self + "?"
      }
    }
  }
}

case object CTStringOrNull extends NullableDefiniteCypherType {
  override def name = CTString + "?"

  override def material = CTString
}

sealed private[spark] trait MaterialDefiniteCypherType extends MaterialCypherType with DefiniteCypherType {
  self =>

  override def material = self

  override def wildcardErasedSuperType: MaterialCypherType with DefiniteCypherType = self

  override def wildcardErasedSubType: MaterialCypherType with DefiniteCypherType = self
}

sealed private[spark] trait NullableDefiniteCypherType extends NullableCypherType with DefiniteCypherType {
  self =>

  override def nullable = self

  override def wildcardErasedSuperType: NullableCypherType with DefiniteCypherType = material.wildcardErasedSuperType.nullable

  override def wildcardErasedSubType: NullableCypherType with DefiniteCypherType = material.wildcardErasedSubType.nullable
}

sealed private[spark] trait MaterialDefiniteCypherLeafType
  extends MaterialDefiniteCypherType with MaterialDefiniteCypherType.DefaultOrNull {

  self =>

  override def superTypeOf(other: CypherType) = other match {
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

