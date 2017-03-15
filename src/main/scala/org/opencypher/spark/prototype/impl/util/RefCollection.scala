package org.opencypher.spark.prototype.impl.util

import org.opencypher.spark.prototype.impl.classy.Register

final case class RefCollection[D](elts: Vector[D])

object RefCollection {
  def empty[D] = RefCollection(Vector.empty[D])

  abstract class AbstractRegister[R, K, D] extends Register[RefCollection[D]] {

    type Ref = R
    type Key = K
    type Def = D

    override def empty: RefCollection[Def] = RefCollection.empty[D]

    override def contents(collection: RefCollection[D]): Traversable[(R, D)] =
      collection.elts.zipWithIndex.map { case ((defn, idx)) => ref(idx) -> defn }

    override def lookup(collection: RefCollection[D], ref: Ref): Option[Def] = {
      val idx = id(ref)
      if (inCollection(collection, idx)) Some(collection.elts(idx)) else None
    }

    override def find(collection: RefCollection[D], defn: Def): Option[Ref] =
      findByKey(collection, key(defn)).filter { ref => collection.elts(id(ref)) == defn }

    override def findByKey(collection: RefCollection[D], k: Key): Option[Ref] = {
      val idx = collection.elts.indexWhere(defn => key(defn) == k)
      if (idx >= 0) Some(ref(idx)) else None
    }

    // left if key(defn) is already inserted at different ref, right otherwise
    override def update(collection: RefCollection[D], ref: R, defn: D): Either[Ref, RefCollection[Def]] = {
      val defnKey = key(defn)
      findByKey(collection, defnKey)
        .filter(altRef => id(altRef) != id(ref))
        .map(altRef => Left(altRef))
        .getOrElse(Right(RefCollection(collection.elts.updated(id(ref), defn))))
    }

    // left if key(defn) is already inserted with a different defn, right otherwise
    override def insert(collection: RefCollection[D], defn: Def): Either[Ref, (Option[RefCollection[Def]], Ref)] = {
      val defnKey = key(defn)
      findByKey(collection, defnKey)
        .map { ref => if (collection.elts(id(ref)) == defn) Right(None -> ref) else Left(ref) }
        .getOrElse { Right(Some(RefCollection(collection.elts :+ defn)) -> ref(collection.elts.size)) }
    }

    protected def id(ref: Ref): Int
    protected def ref(id: Int): Ref

    private def inCollection(collection: RefCollection[D], idx: Int) = idx >= 0 && idx <= collection.elts.size

    override def remove(collection: RefCollection[D], ref: R): Option[RefCollection[D]] =
      lookup(collection, ref).map { defn =>
        RefCollection(collection.elts.filter(_ != defn))
      }
  }
}
