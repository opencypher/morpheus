/*
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
package org.opencypher.caps.impl.logical

import org.scalatest.matchers.{MatchResult, Matcher}

case class structurallyEqual(right: Any) extends Matcher[Any] {
  override def apply(left: Any): MatchResult = {
    structurallyEqualAny(left, right)
  }

  private def structurallyEqualAny(left: Any, right: Any): MatchResult = {
    if (left == right) {
      success(left, right)
    } else {
      notEqual(left, right)
      if (left.getClass != right.getClass) {
        failure(s"${left.getClass.getSimpleName} did not equal ${right.getClass.getSimpleName}")
      } else {
        left match {
          case p: Product   => structurallyEqualProduct(p, right.asInstanceOf[Product])
          case t: Seq[_]    => structurallyEqualTraversable(t, t.asInstanceOf[Seq[_]])
          case _            => failure(s"$left did not equal $right")
        }
      }
    }
  }

  private def structurallyEqualTraversable(left: Traversable[_], right: Traversable[_]): MatchResult = {
    if (left == right) {
      success(left, right)
    } else {
      notEqual(left, right)
      if (left.getClass != right.getClass) {
        failure(s"${left.getClass.getSimpleName} did not equal ${right.getClass.getSimpleName}")
      } else {
        combine(
          left.toSeq
            .zip(right.toSeq)
            .map {
              case (l, r) =>
                structurallyEqualAny(l, r)
            }: _*)
      }
    }
  }

  private def structurallyEqualProduct(left: Product, right: Product): MatchResult = {
    if (left == right) {
      success(left, right)
    } else {
      notEqual(left, right)
      if (!left.productPrefix.equals(right.productPrefix)) {
        failure(s"${left.productPrefix} does not equal ${right.productPrefix}")
      } else {
        if (left.productIterator.size != right.productIterator.size) {
          failure(s"Inside of ${left.productPrefix} ${left.productIterator
            .mkString(", ")} does not equal ${left.productIterator.mkString(", ")}}")
        } else {
          combine(
            left.productIterator.toSeq
              .zip(right.productIterator.toSeq)
              .map {
                case (l, r) =>
                  structurallyEqualAny(l, r)
              }: _*)
        }
      }
    }
  }

  def failure(msg: String) = MatchResult(false, msg, "")

  def notEqual(l: Any, r: Any): Unit = {
    //println(s"${AsCode(l)} did NOT equal ${AsCode(r)}")
  }

  def success(l: Any, r: Any) = MatchResult(true, "", "")

  // Returns a successful or the first failed match if there is one.
  def combine(m: MatchResult*): MatchResult = {
    m.foldLeft(MatchResult(true, "", "")) {
      case (aggr, next) =>
        if (aggr.matches) next else aggr
    }
  }

}
