package org.opencypher.spark.prototype.impl.classy

// Manage collections of elements of type Def such that the collection holds at most one element
// with a key of type Key.
//
// Elements may be addressed efficiently using references of type Ref
//
trait Register[Collection] {

  self =>

  type Ref
  type Key
  type Def

  def empty: Collection
  def contents(collection: Collection): Traversable[(Ref, Def)]

  def key(defn: Def): Key

  def find(collection: Collection, defn: Def): Option[Ref]
  def lookup(collection: Collection, key: Key): Option[Ref]
  def get(collection: Collection, ref: Ref): Option[Def]

  // left if key(defn) is already inserted at different ref, right otherwise
  def update(collection: Collection, ref: Ref, defn: Def): Either[Ref, Collection]

  // left if key(defn) is already inserted with a different defn, right otherwise
  def insert(collection: Collection, defn: Def): Either[Ref, (Option[Collection], Ref)]

  // none if not contained. All previous refs are invalid after a remove!
  def remove(collection: Collection, ref: Ref): Option[Collection]
}

object Register {
  @inline
  final def apply[C, R, K, D](implicit register: Register[C] { type Ref = R; type Key = K; type Def = D }) = register
}
