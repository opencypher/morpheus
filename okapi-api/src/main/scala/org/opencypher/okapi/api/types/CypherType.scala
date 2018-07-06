package org.opencypher.okapi.api.types

object CypherType extends App {

  val CTNumber: CypherType = CTInteger union CTFloat
  val CTVoid: CypherType = UnionType(Set.empty)

  val namedTypes = Map(
    CTInteger.alternatives -> "CTInteger",
    CTFloat.alternatives -> "CTFloat",
    CTNumber.alternatives -> "CTNumber",
    CTVoid.alternatives -> "CTVoid",
    CTNull.alternatives -> "CTNull"
  )

  println(s"CTInteger = $CTInteger")
  println(s"CTInteger.nullable = ${CTInteger.nullable}")
  println(s"CTFloat = $CTFloat")
  println(s"CTNull = $CTNull")
  println(s"CTVoid = $CTVoid")
  println(s"CTInteger union CTFloat = ${CTInteger union CTFloat}")
  println(s"(CTInteger union CTFloat).nullable = ${(CTInteger union CTFloat).nullable}")
  println(s"CTInteger intersect CTFloat = ${CTInteger intersect CTFloat}")

}

// BASIC
trait BasicType extends CypherType {
  override def alternatives: Set[CypherType] = Set(this)
}

case object CTInteger extends BasicType

case object CTFloat extends BasicType

case object CTNull extends BasicType

// UNION
case class UnionType(alternatives: Set[CypherType]) extends CypherType


trait CypherType {

  def alternatives: Set[CypherType]

  def material: Set[CypherType] = alternatives.filter(_ != CTNull)

  def intersect(other: CypherType): CypherType = {
    if (this == other) {
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
  def join(other: CypherType): CypherType = intersect(other)

  def union(other: CypherType): CypherType = {
    if (this == other) {
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
  def meet(other: CypherType): CypherType = union(other)

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

  def name: String = {
    val materialName = CypherType.namedTypes.get(material) match {
      case Some(n) => n
      case None => alternatives.mkString("[", "|", "]")
    }
    if (isNullable) {
      s"$materialName.nullable"
    } else {
      materialName
    }
  }

  override def toString: String = name

  override def equals(other: Any): Boolean =
    other match {
      case basic: BasicType => this.eq(basic)
      case that: CypherType => this.eq(that) || alternatives == that.alternatives
      case _ => false
    }

  override def hashCode: Int = {
    this match {
      case _: UnionType => alternatives.hashCode
      case _ => super.hashCode
    }
  }

}
