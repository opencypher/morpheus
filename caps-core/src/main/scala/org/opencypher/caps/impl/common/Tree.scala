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
package org.opencypher.caps.impl.common

case class Tree[+T](value: T, children: Seq[Tree[T]] = Seq.empty) extends Traversable[T] {

  def arity: Int = children.length

  def isLeaf: Boolean = height == 1

  def isInner: Boolean = height > 1

  lazy val height: Int = if (children.isEmpty) 1 else children.map(_.height).max + 1

  protected def prefix(depth: Int): String = ("· " * depth) + "|-"

  def aggregate[O](f: (T, Seq[O]) => O): O = {
    f(value, children.map(_.aggregate(f)))
  }

  def map[O](f: T => O): Tree[O] = {
    copy(f(value), children.map(c => c.map(f)))
  }

  def pretty(depth: Int = 0): String = {
    s"${prefix(depth)}($value)\n${children.map(_.pretty(depth + 1)).mkString("")}"
  }

  override def foldLeft[O](initial: O)(f: (O, T) => O): O = {
    children.foldLeft(f(initial, value)) { case (agg, nextChild) =>
      nextChild.foldLeft(agg)(f)
    }
  }

  override def foreach[O](f: T => O): Unit = {
    f(value)
    children.foreach(_.foreach(f))
  }

}

object Tree {

  trait Aggregate[I, O] extends ((I, Seq[O]) => O) {
    def apply(operator: I, inputs: Seq[O]): O

    def apply(t: Tree[I]): O = t.aggregate(this)
  }

  trait Rewrite[T] extends (Tree[T] => Tree[T])

  trait ReplaceChildren[T] extends Rewrite[T] {
    def apply(n: Tree[T]): Tree[T] = {
      val updatedChildren = if (!shouldUpdate(n)) {
        n.children
      } else {
        n.children.map { c =>
          if (!shouldReplace(c)) c
          else replace(c)
        }
      }
      n.copy(children = updatedChildren.map(this (_)))
    }

    def shouldUpdate(parent: Tree[T]): Boolean = true

    def shouldReplace(childNode: Tree[T]): Boolean = true

    def replace(childNode: Tree[T]): Tree[T]
  }

}
