package org.opencypher.spark.impl.prototype

object TokenRegistration {

  type Registry[D <: TokenDef] = IndexedSeq[D]

  implicit object labelRegistration extends TokenRegistration[LabelRef, LabelDef]("label") {
    override protected def createRef(id: Int): LabelRef = LabelRef(id)
  }

  implicit object relTypeRegistration extends TokenRegistration[RelTypeRef, RelTypeDef]("rel-type") {
    override protected def createRef(id: Int): RelTypeRef = RelTypeRef(id)
  }

  implicit object propertyKeyRegistration extends TokenRegistration[PropertyKeyRef, PropertyKeyDef]("property-key") {
    override protected def createRef(id: Int): PropertyKeyRef = PropertyKeyRef(id)
  }

  implicit final class TokenRegistryOps[R <: TokenRef[D], D <: TokenDef](val registry: Registry[D]) extends AnyVal {

    type Registration = TokenRegistration[R, D]

    def ref(defn: D)(implicit registration: Registration): R = registration.ref(registry, defn)
    def ref(name: String)(implicit registration: Registration): R = registration.ref(registry, name)
    def get(ref: R)(implicit registration: Registration): D = registration.get(registry, ref)
    def merge(defn: D)(implicit registration: Registration): Registry[D] = registration.merge(registry, defn)
  }

  def apply[R <: TokenRef[D], D <: TokenDef](implicit registration: TokenRegistration[R, D]): TokenRegistration[R, D] = registration
}

abstract class TokenRegistration[R <: TokenRef[D], D <: TokenDef](val tokenKindName: String) {

  import TokenRegistration.Registry

  def empty: Registry[D] = Vector.empty

  def ref(registry: Registry[D], defn: D): R =
    ref(registry, defn.name)

  def ref(registry: Registry[D], name: String): R = {
    val idx = registry.indexWhere(_.name == name)
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName '$name'") else createRef(idx)
  }

  def get(registry: Registry[D], ref: R): D = {
    val idx = ref.id
    if (idx < 0) throw new AssertionError(s"Invalid $tokenKindName reference with id $idx") else registry(idx)
  }

  def merge(registry: Registry[D], defn: D): Registry[D] = {
    val idx = registry.indexOf(defn)
    if (idx >= 0) registry else registry :+ defn
  }

  protected def createRef(id: Int): R
}
