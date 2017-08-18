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
package org.opencypher.caps.impl.syntax.util

import cats.Monoid
import cats.syntax.monoid._

import scala.language.implicitConversions

trait TraversableSyntax {
  implicit def traversableSyntax[E](elts: Traversable[(E)]): TraversableOps[E] =
    new TraversableOps[E](elts)
}

final class TraversableOps[E](val elts: Traversable[E]) {
  def groups[K, V](implicit ev: E =:= (K, V), monoid: Monoid[V]): Map[K, V] =
    groupsFrom(ev)

  def groupsFrom[K, V](f: E => (K, V))(implicit monoid: Monoid[V]): Map[K, V] = {
    elts.foldLeft(Map.empty[K, V]) {
      case (m, elt) =>
        val (k, v) = f(elt)
        m.get(k) match {
          case Some(values) => m.updated(k, values |+| v)
          case None => m.updated(k, v)
        }
    }
  }
}
