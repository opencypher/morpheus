package org.opencypher.spark.prototype.ir.token

object TokenCollector {

  type TokenCollection[D <: Token] = IndexedSeq[D]

  implicit object parameterCollector extends TokenCollector[ParameterRef, Parameter]("parameter") {
    override protected def createRef(id: Int): ParameterRef = ParameterRef(id)
  }

  implicit object labelCollector extends TokenCollector[LabelRef, Label]("label") {
    override protected def createRef(id: Int): LabelRef = LabelRef(id)
  }

  implicit object relTypeCollector extends TokenCollector[RelTypeRef, RelType]("rel-type") {
    override protected def createRef(id: Int): RelTypeRef = RelTypeRef(id)
  }

  implicit object propertyKeyCollector extends TokenCollector[PropertyKeyRef, PropertyKey]("property-key") {
    override protected def createRef(id: Int): PropertyKeyRef = PropertyKeyRef(id)
  }

  implicit final class TokenCollectorOps[R <: TokenRef[D], D <: Token](val tokens: TokenCollection[D])
    extends AnyVal {

    type Collector = TokenCollector[R, D]

    def ref(defn: D)(implicit collector: Collector): R = collector.ref(tokens, defn)
    def ref(name: String)(implicit collector: Collector): R = collector.ref(tokens, name)
    def get(ref: R)(implicit collector: Collector): D = collector.get(tokens, ref)
    def merge(defn: D)(implicit collector: Collector): TokenCollection[D] = collector.merge(tokens, defn)
  }

  def apply[R <: TokenRef[D], D <: Token](implicit collector: TokenCollector[R, D]): TokenCollector[R, D] = collector
}

sealed abstract class TokenCollector[R <: TokenRef[D], D <: Token](val tokenKindName: String) {

  import TokenCollector.TokenCollection

  def empty: TokenCollection[D] = Vector.empty

  def ref(collection: TokenCollection[D], defn: D): R =
    ref(collection, defn.name)

  def ref(collection: TokenCollection[D], name: String): R = {
    val idx = collection.indexWhere(_.name == name)
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName '$name'") else createRef(idx)
  }

  def get(collection: TokenCollection[D], ref: R): D = {
    val idx = ref.id
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName reference with id $idx") else collection(idx)
  }

  def merge(collection: TokenCollection[D], defn: D): TokenCollection[D] = {
    val idx = collection.indexOf(defn)
    if (idx >= 0) collection else collection :+ defn
  }

  protected def createRef(id: Int): R
}
