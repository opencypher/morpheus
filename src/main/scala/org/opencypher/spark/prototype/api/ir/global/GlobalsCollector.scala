package org.opencypher.spark.prototype.api.ir.global

object GlobalsCollector {

  type GlobalsCollection[D <: Global] = IndexedSeq[D]

  implicit object constantCollector extends GlobalsCollector[ConstantRef, Constant]("parameter") {
    override protected def createRef(id: Int): ConstantRef = ConstantRef(id)
  }

  implicit object labelCollector extends GlobalsCollector[LabelRef, Label]("label") {
    override protected def createRef(id: Int): LabelRef = LabelRef(id)
  }

  implicit object relTypeCollector extends GlobalsCollector[RelTypeRef, RelType]("rel-type") {
    override protected def createRef(id: Int): RelTypeRef = RelTypeRef(id)
  }

  implicit object propertyKeyCollector extends GlobalsCollector[PropertyKeyRef, PropertyKey]("property-key") {
    override protected def createRef(id: Int): PropertyKeyRef = PropertyKeyRef(id)
  }

  implicit final class TokenCollectorOps[R <: GlobalRef[D], D <: Global](val tokens: GlobalsCollection[D])
    extends AnyVal {

    type Collector = GlobalsCollector[R, D]

    def ref(defn: D)(implicit collector: Collector): R = collector.ref(tokens, defn)
    def ref(name: String)(implicit collector: Collector): R = collector.ref(tokens, name)
    def get(ref: R)(implicit collector: Collector): D = collector.get(tokens, ref)
    def merge(defn: D)(implicit collector: Collector): GlobalsCollection[D] = collector.merge(tokens, defn)
  }

  def apply[R <: GlobalRef[D], D <: Global](implicit collector: GlobalsCollector[R, D]): GlobalsCollector[R, D] = collector
}

sealed abstract class GlobalsCollector[R <: GlobalRef[D], D <: Global](val tokenKindName: String) {

  import GlobalsCollector.GlobalsCollection

  def empty: GlobalsCollection[D] = Vector.empty

  def ref(collection: GlobalsCollection[D], defn: D): R =
    ref(collection, defn.name)

  def ref(collection: GlobalsCollection[D], name: String): R = {
    val idx = collection.indexWhere(_.name == name)
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName '$name'") else createRef(idx)
  }

  def get(collection: GlobalsCollection[D], ref: R): D = {
    val idx = ref.id
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName reference with id $idx") else collection(idx)
  }

  def merge(collection: GlobalsCollection[D], defn: D): GlobalsCollection[D] = {
    val idx = collection.indexOf(defn)
    if (idx >= 0) collection else collection :+ defn
  }

  protected def createRef(id: Int): R
}
