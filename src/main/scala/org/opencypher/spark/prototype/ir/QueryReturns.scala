package org.opencypher.spark.prototype.ir

final case class QueryReturns(items: Seq[(String, Field)]) {
  def foldLeft[A](acc: A)(f: (A, (String, Field)) => A): A = {
    items.foldLeft(acc)(f)
  }
  def map[A](f: ((String, Field)) => A): Seq[A] = items.map(f)
}

case object QueryReturns {
  val nothing = QueryReturns(Seq.empty)
}
