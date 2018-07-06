package org.opencypher.okapi.api.types

object CypherType extends App {

  val CTNumber: CypherType = CTInteger union CTFloat
  val CTVoid: CypherType = UnionType(Set.empty)

  println(s"CTInteger = $CTInteger")
  println(s"CTInteger.nullable = ${CTInteger.nullable}")
  println(s"CTFloat = $CTFloat")
  println(s"CTNull = $CTNull")
  println(s"CTVoid = $CTVoid")
  println(s"CTInteger union CTFloat = ${CTInteger union CTFloat}")
  println(s"(CTInteger union CTFloat).nullable = ${(CTInteger union CTFloat).nullable}")
  println(s"CTInteger intersect CTFloat = ${CTInteger intersect CTFloat}")
  println(s"CTList(CTInteger union CTFloat) = ${CTList(CTInteger union CTFloat)}")
  println(s"CTWildcard = ${CTWildcard}")
  println(s"CTWildcard union CTInteger = ${CTWildcard union CTInteger}")
  println(s"CTWildcard intersect CTInteger = ${CTWildcard intersect CTInteger}")
  println(s"CTNode(Set(A)) = ${CTNode(Set("A"))}")
  println(s"CTNode(Set(A)) isSubTypeOf CTNode(Set(A, B)) = ${CTNode(Set("A")) isSubTypeOf CTNode(Set("A", "B"))}")
  println(s"CTNode(Set(A)) isSuperTypeOf CTNode(Set(A, B)) = ${CTNode(Set("A")) isSuperTypeOf CTNode(Set("A", "B"))}")

}

// BASIC
trait BasicType extends CypherType {
  override def alternatives: Set[CypherType] = Set(this)

  override def name: String = getClass.getSimpleName.filterNot(_ == '$')
}

case object CTInteger extends BasicType

case object CTFloat extends BasicType

case object CTNull extends BasicType

case object CTWildcard extends BasicType

case class CTNode(labels: Set[String]) extends BasicType {
  override def isSubTypeOf(other: CypherType): Boolean = {
    other match {
      case CTWildcard => true
      case CTNode(otherLabels) if labels subsetOf otherLabels => true
      case _ => false
    }
  }

  override def isSuperTypeOf(other: CypherType): Boolean = {
    other match {
      case CTWildcard => false
      case CTNode(otherLabels) if otherLabels subsetOf labels => true
      case _ => false
    }
  }

  override def intersect(other: CypherType): CypherType = super.intersect(other)
}

case class CTList(elementType: CypherType) extends BasicType {
  override def name: String = s"CTList$elementType"
}

// UNION
case class UnionType(alternatives: Set[CypherType]) extends CypherType


trait CypherType {

  def alternatives: Set[CypherType]

  def material: Set[CypherType] = alternatives.filterNot(_ == CTNull)

  def isSubTypeOf(other: CypherType): Boolean = {
    if (other == CTWildcard) {
      true
    } else {
      alternatives.subsetOf(other.alternatives)
    }
  }

  def isSuperTypeOf(other: CypherType): Boolean = {
    if (this == CTWildcard) {
      true
    } else {
      other.alternatives.subsetOf(alternatives)
    }
  }

  def intersect(other: CypherType): CypherType = {
    if (this == CTWildcard) {
      other
    } else if (other == CTWildcard) {
      this
    } else if (this == other) {
      this
    } else if (alternatives.contains(other)) {
      other
    } else if (other.alternatives.contains(this)) {
      this
    } else {
      UnionType(alternatives intersect other.alternatives)
    }
  }

  @deprecated
  def meet(other: CypherType): CypherType = intersect(other)

  def union(other: CypherType): CypherType = {
    if (this == CTWildcard || other == CTWildcard) {
      CTWildcard
    } else if (this == other) {
      this
    } else if (alternatives.contains(other)) {
      this
    } else if (other.alternatives.contains(this)) {
      other
    } else {
      UnionType(alternatives union other.alternatives)
    }
  }

  @deprecated
  def join(other: CypherType): CypherType = union(other)

  def isNullable: Boolean = {
    alternatives.contains(CTNull)
  }

  def nullable: CypherType = {
    if (this.alternatives.contains(CTNull)) {
      this
    } else {
      UnionType(alternatives + CTNull)
    }
  }

  def name: String = alternatives.map(_.name).mkString("|")

  override def toString: String = s"[$name]"

  override def equals(other: Any): Boolean = {
    other match {
      case otherBasic: BasicType => this.eq(otherBasic) || {
        this match {
          case _: BasicType => false
          case _ => this.alternatives.size == 1 && this.alternatives.head == other
        }
      }
      case that: CypherType => this.eq(that) || alternatives == that.alternatives
      case _ => false
    }
  }

  override def hashCode: Int = {
    this match {
      case _: UnionType => alternatives.hashCode
      case _ => super.hashCode
    }
  }

}
