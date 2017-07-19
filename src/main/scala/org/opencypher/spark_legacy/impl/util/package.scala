/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.impl

package object util {

  object ZeroProduct extends Product {
    override def productElement(n: Int): Any = throw new IndexOutOfBoundsException(n.toString)
    override def productArity: Int = 0
    override def canEqual(that: Any): Boolean = false
  }

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

  final case class appendConstant[T](v: T) extends (Product => Product) {
    override def apply(in: Product): Product = in :+ v
  }

  implicit final class RichProduct(val product: Product) extends AnyVal {

    def drop(index: Int) = {
      val arity = product.productArity

      if (arity < index)
        throw new IllegalArgumentException(s"Can't drop field $index from a product of arity $arity")

      (0 until arity).filter(_ != index).map { i =>
        product.productElement(i)
      }.asProduct
    }

    def :+(elt: Any) = product.productArity match {
      case 0 =>
        Tuple1(elt)

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

      case 9 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, elt)

      case 10 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, elt)

      case 11 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, elt)

      case 12 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, elt)

      case 13 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, elt)

      case 14 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, elt)

      case 15 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, elt)

      case 16 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, src._16, elt)

      case 17 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, src._16, src._17, elt)

      case 18 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, src._16, src._17, src._18, elt)

      case 19 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, src._16, src._17, src._18, src._19, elt)

      case 20 =>
        val src = product.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any)]
        (src._1, src._2, src._3, src._4, src._5, src._6, src._7, src._8, src._9, src._10, src._11, src._12, src._13, src._14, src._15, src._16, src._17, src._18, src._19, src._20, elt)

      case x =>
        throw new UnsupportedOperationException(s"Implement support for larger products: $x")
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
