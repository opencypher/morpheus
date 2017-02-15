package org.opencypher.spark.impl.prototype

object TokenDefs {
  val empty = TokenDefs(Vector.empty, Vector.empty, Vector.empty)
}

final case class TokenDefs(
  labels: IndexedSeq[LabelDef],
  relTypes: IndexedSeq[RelTypeDef],
  propertyKeys: IndexedSeq[PropertyKeyDef]
) {

  def label(name: String): LabelRef = LabelRef(idx(labels.indexWhere(_.name == name)).get)
  def label(ref: LabelRef): LabelDef = labels(ref.id)

  def relType(name: String): RelTypeRef = RelTypeRef(idx(relTypes.indexWhere(_.name == name)).get)
  def relType(ref: RelTypeRef): RelTypeDef = relTypes(ref.id)

  def propertyKey(name: String): PropertyKeyRef = PropertyKeyRef(idx(propertyKeys.indexWhere(_.name == name)).get)
  def propertyKey(ref: PropertyKeyRef): PropertyKeyDef = propertyKeys(ref.id)

  def withLabel(defn: LabelDef): TokenDefs = {
    val (tokens, _) = withLabelAndRef(defn)
    tokens
  }

  def withLabelAndRef(defn: LabelDef): (TokenDefs, LabelRef) = idx(labels.indexOf(defn)) match {
    case None =>
      val newLabels = labels :+ defn
      val newTokens = copy(labels = newLabels)
      val newRef = LabelRef(newLabels.size - 1)
      newTokens -> newRef

    case Some(idx) =>
      this -> LabelRef(idx)
  }

  def withRelType(defn: RelTypeDef): TokenDefs = {
    val (tokens, _) = withRelTypeAndRef(defn)
    tokens
  }

  def withRelTypeAndRef(defn: RelTypeDef): (TokenDefs, RelTypeRef) = idx(relTypes.indexOf(defn)) match {
    case None =>
      val newRelTypes = relTypes :+ defn
      val newTokens = copy(relTypes = newRelTypes)
      val newRef = RelTypeRef(newRelTypes.size - 1)
      newTokens -> newRef

    case Some(idx) =>
      this -> RelTypeRef(idx)
  }

  def withPropertyKey(defn: PropertyKeyDef): TokenDefs = {
    val (tokens, _) = withPropertyKeyAndRef(defn)
    tokens
  }

  def withPropertyKeyAndRef(defn: PropertyKeyDef): (TokenDefs, PropertyKeyRef) = idx(propertyKeys.indexOf(defn)) match {
    case None =>
      val newPropertyKeys = propertyKeys :+ defn
      val newTokens = copy(propertyKeys = newPropertyKeys)
      val newRef = PropertyKeyRef(newPropertyKeys.size - 1)
      newTokens -> newRef

    case Some(idx) =>
      this -> PropertyKeyRef(idx)
  }

  private def idx(idx: Int) = if (idx == -1) None else Some(idx)
}
