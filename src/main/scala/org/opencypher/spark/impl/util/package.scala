package org.opencypher.spark.impl

package object util {

  implicit final class RichIndexedSeq[+T](val elts: IndexedSeq[T]) extends AnyVal {

    def asProduct: Product = elts.length match {
      case 0 => throw new IllegalArgumentException("Can't turn empty sequence into a tuple")
      case 1 => Tuple1(elts(0))
      case 2 => Tuple2(elts(0), elts(1))
      case 3 => Tuple3(elts(0), elts(1), elts(2))
      case 4 => Tuple4(elts(0), elts(1), elts(2), elts(3))
      case 5 => Tuple5(elts(0), elts(1), elts(2), elts(3), elts(4))
      case 6 => Tuple6(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5))
      case 7 => Tuple7(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6))
      case 8 => Tuple8(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6), elts(7))
      case 9 => Tuple9(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6), elts(7), elts(8))
      case _ => throw new UnsupportedOperationException("Implement support for larger products")
    }
  }

  implicit final class RichProduct(val product: Product) extends AnyVal {

    def :+(elt: Any) = product.productArity match {
      case 0 =>
        throw new IllegalArgumentException("Invalid input product (tuple expected as zero arity products are unsupported)")

      case 1 =>
        val src = product.asInstanceOf[Tuple1[Any]]
        (src._1, elt)

      case 2 =>
        val src = product.asInstanceOf[(Any, Any)]
        (src._1, src._2, elt)

      case 3 =>
        val src = product.asInstanceOf[(Any, Any, Any)]
        (src._1, src._2, src._3, elt)

      case 4 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, elt)

      case 5 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, elt)

      case 6 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, elt)

      case 7 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, elt)

      case 8 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, elt)

      case _ =>
        throw new UnsupportedOperationException("Implement support for larger products")
    }

    def getAs[T](index: Int): T = get(index).asInstanceOf[T]

    def get(index: Int): Any = product.productElement(index)

    def asVector: Vector[Any] = product.productArity match {
      case 0 =>
        Vector.empty

      case 1 =>
        Vector(product.asInstanceOf[Tuple1[Any]]._1)

      case 2 =>
        val t = product.asInstanceOf[(Any, Any)]
        Vector(t._1, t._2)

      case 3 =>
        val t = product.asInstanceOf[(Any, Any, Any)]
        Vector(t._1, t._2, t._3)

      case 4 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4)

      case 5 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4, t._5)

      case 6 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4, t._5, t._6)

      case 7 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4, t._5, t._6, t._7)

      case 8 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8)

      case 9 =>
        val t = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        Vector(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9)

      case _ =>
        throw new UnsupportedOperationException("Implement support for larger products")
    }
  }
}
