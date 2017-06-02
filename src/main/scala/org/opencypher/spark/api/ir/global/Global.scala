package org.opencypher.spark.api.ir.global

import org.opencypher.spark.impl.util.RefCollection.AbstractRegister

sealed trait Global extends Any {
  def name: String
}

object Global {

  implicit val labelRegister = new AbstractRegister[LabelRef, String, Label] {
    override protected def id(ref: LabelRef): Int = ref.id
    override protected def ref(id: Int): LabelRef = LabelRef(id)
    override def key(defn: Label): String = defn.name
  }

  implicit val relTypeRegister = new AbstractRegister[RelTypeRef, String, RelType] {
    override protected def id(ref: RelTypeRef): Int = ref.id
    override protected def ref(id: Int): RelTypeRef = RelTypeRef(id)
    override def key(defn: RelType): String = defn.name
  }

  implicit val pkRegister = new AbstractRegister[PropertyKeyRef, String, PropertyKey] {
    override protected def id(ref: PropertyKeyRef): Int = ref.id
    override protected def ref(id: Int): PropertyKeyRef = PropertyKeyRef(id)
    override def key(defn: PropertyKey): String = defn.name
  }

  implicit val constRegister = new AbstractRegister[ConstantRef, String, Constant] {
    override protected def id(ref: ConstantRef): Int = ref.id
    override protected def ref(id: Int): ConstantRef = ConstantRef(id)
    override def key(defn: Constant): String = defn.name
  }
}

sealed trait GlobalRef[D <: Global] extends Any {
  def id: Int
}

final case class Label(name: String) extends AnyVal with Global
final case class LabelRef(id: Int) extends AnyVal with GlobalRef[Label]

final case class PropertyKey(name: String) extends AnyVal with Global
final case class PropertyKeyRef(id: Int) extends AnyVal with GlobalRef[PropertyKey]

final case class RelType(name: String) extends AnyVal with Global
final case class RelTypeRef(id: Int) extends AnyVal with GlobalRef[RelType]

final case class Constant(name: String) extends AnyVal with Global
final case class ConstantRef(id: Int) extends AnyVal with GlobalRef[Constant]



