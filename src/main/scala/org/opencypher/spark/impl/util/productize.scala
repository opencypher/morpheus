package org.opencypher.spark.impl.util

case object productize {
  def apply[T](elts: Seq[T]): Product = elts.length match {
    case 0 => throw new IllegalArgumentException("Can't turn empty sequence into a tuple")
    case 1 => elts match { case Seq(e1) => Tuple1(e1) }
    case 2 => elts match { case Seq(e1, e2) => Tuple2(e1, e2) }
    case 3 => elts match { case Seq(e1, e2, e3) => Tuple3(e1, e2, e3) }
    case 4 => elts match { case Seq(e1, e2, e3, e4) => Tuple4(e1, e2, e3, e4) }
    case 5 => elts match { case Seq(e1, e2, e3, e4, e5) => Tuple5(e1, e2, e3, e4, e5) }
    case 6 => elts match { case Seq(e1, e2, e3, e4, e5, e6) => Tuple6(e1, e2, e3, e4, e5, e6) }
    case 7 => elts match { case Seq(e1, e2, e3, e4, e5, e6, e7) => Tuple7(e1, e2, e3, e4, e5, e6, e7) }
    case 8 => elts match { case Seq(e1, e2, e3, e4, e5, e6, e7, e8) => Tuple8(e1, e2, e3, e4, e5, e6, e7, e8) }
    case 9 => elts match { case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9) => Tuple9(e1, e2, e3, e4, e5, e6, e7, e8, e9) }
    case _ => throw new UnsupportedOperationException("implement a larger tuple")
  }
}
