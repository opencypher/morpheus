package org.opencypher.spark.api.ir.global

sealed trait Global {
  def name: String
}

sealed trait GlobalRef[D <: Global] {
  def id: Int
}

final case class Label(name: String) extends Global
final case class LabelRef(id: Int) extends GlobalRef[Label]

final case class PropertyKey(name: String) extends Global
final case class PropertyKeyRef(id: Int) extends GlobalRef[PropertyKey]

final case class RelType(name: String) extends Global
final case class RelTypeRef(id: Int) extends GlobalRef[RelType]

final case class Constant(name: String) extends Global
final case class ConstantRef(id: Int) extends GlobalRef[Constant]



