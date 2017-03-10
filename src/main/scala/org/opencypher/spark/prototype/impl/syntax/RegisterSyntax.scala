package org.opencypher.spark.prototype.impl.syntax

import org.opencypher.spark.prototype.impl.classy.Register

trait RegisterSyntax {
  def key[D, K](defn: D)(implicit register: Register[_] { type Def = D; type Key = K }) =
    register.key(defn)

  implicit def registerSyntax[C, R, K, D](coll: C)
  (implicit
   register: Register[C] { type Ref = R; type Key = K; type Def = D }
  ) =
    new RegisterOps[C, R, K, D](coll)
}

final class RegisterOps[C, R, K, D](coll: C)
(implicit
  val register: Register[C] { type Ref = R; type Key = K; type Def = D }
) {
  def contents: Traversable[(R, D)] = register.contents(coll)

  def get(ref: R): Option[D] = register.get(coll, ref)
  def find(defn: D): Option[R] = register.find(coll, defn)
  def lookup(key: K): Option[R] = register.lookup(coll, key)
  def insert(defn: D): Either[R, (Option[C], R)] = register.insert(coll, defn)
  def update(ref: R, defn: D): Either[R, C] = register.update(coll, ref, defn)
}
