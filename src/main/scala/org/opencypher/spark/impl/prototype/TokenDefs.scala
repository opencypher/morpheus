package org.opencypher.spark.impl.prototype

object TokenDefs {
  val empty = TokenDefs(Vector.empty, Vector.empty, Vector.empty)
}

case class TokenDefs(
  labels: IndexedSeq[LabelDef],
  relTypes: IndexedSeq[RelTypeDef],
  propertyKeys: IndexedSeq[PropertyKeyDef]
) {

  def label(name: String): Option[LabelRef] = idx(labels.indexWhere(_.name == name)).map(LabelRef)
  def label(ref: LabelRef): Option[LabelDef] = labels.lift(ref.id)

  def relType(name: String): Option[RelTypeRef] = idx(relTypes.indexWhere(_.name == name)).map(RelTypeRef)
  def relType(ref: RelTypeRef): Option[RelTypeDef] = relTypes.lift(ref.id)

  def propertyKey(name: String): Option[PropertyKeyRef] = idx(propertyKeys.indexWhere(_.name == name)).map(PropertyKeyRef)
  def propertyKey(ref: PropertyKeyRef): Option[PropertyKeyDef] = propertyKeys.lift(ref.id)

  def withLabel(defn: LabelDef): (LabelRef, TokenDefs) = idx(labels.indexOf(defn)) match {
    case None =>
      val newLabels = labels :+ defn
      val newTokens = copy(labels = newLabels)
      val newRef = LabelRef(newLabels.size - 1)
      newRef -> newTokens

    case Some(idx) =>
      LabelRef(idx) -> this
  }

  def withRelType(defn: RelTypeDef): (RelTypeRef, TokenDefs) = idx(relTypes.indexOf(defn)) match {
    case None =>
      val newRelTypes = relTypes :+ defn
      val newTokens = copy(relTypes = newRelTypes)
      val newRef = RelTypeRef(newRelTypes.size - 1)
      newRef -> newTokens

    case Some(idx) =>
      RelTypeRef(idx) -> this
  }

  def withPropertyKey(defn: PropertyKeyDef): (PropertyKeyRef, TokenDefs) = idx(propertyKeys.indexOf(defn)) match {
    case None =>
      val newPropertyKeys = propertyKeys :+ defn
      val newTokens = copy(propertyKeys = newPropertyKeys)
      val newRef = PropertyKeyRef(newPropertyKeys.size - 1)
      newRef -> newTokens

    case Some(idx) =>
      PropertyKeyRef(idx) -> this
  }

  private def idx(idx: Int) = if (idx == -1) None else Some(idx)
}
