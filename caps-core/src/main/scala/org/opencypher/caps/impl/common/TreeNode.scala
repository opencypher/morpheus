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

import scala.util.hashing.MurmurHash3

abstract class TreeNode[T <: TreeNode[T]] extends Product with Traversable[T] {

  self: T =>

  def withNewChildren(newChildren: Seq[T]): T

  def children: Seq[T] = Seq.empty

  // Optimization: Cache hash code, speeds up repeated computations over high trees.
  override lazy val hashCode = MurmurHash3.productHash(this)

  def arity: Int = children.length

  def isLeaf: Boolean = height == 1

  def isInner: Boolean = height > 1

  lazy val height: Int = if (children.isEmpty) 1 else children.map(_.height).max + 1

  def map[O <: TreeNode[O]](f: T => O): O = {
    f(self).withNewChildren(children.map(f))
  }

  override def foldLeft[O](initial: O)(f: (O, T) => O): O = {
    children.foldLeft(f(initial, this)) { case (agg, nextChild) =>
      nextChild.foldLeft(agg)(f)
    }
  }

  override def foreach[O](f: T => O): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  def containsTree(n: T): Boolean = {
    if (self == n) true else children.exists(_.containsTree(n))
  }

  def transformUp(rule: TreeNode.RewriteRule[T]): T = {
    val afterChildren = withNewChildren(children.map(_.transformUp(rule)))
    if (rule.isDefinedAt(afterChildren)) rule(afterChildren) else afterChildren
  }

  def transformDown(rule: TreeNode.RewriteRule[T]): T = {
    val afterSelf = if (rule.isDefinedAt(self)) rule(self) else self
    afterSelf.withNewChildren(afterSelf.children.map(_.transformDown(rule)))
  }

  protected def prefix(depth: Int): String = ("· " * depth) + "|-"

  def pretty(depth: Int = 0): String = {

    val childrenString = children.foldLeft("") {
      case (agg, s) => agg + s.pretty(depth + 1)
    }

    s"${prefix(depth)}($self)\n$childrenString"
  }

  def argString: String = productIterator.filter(argFilter).flatMap {
    case tn: TreeNode[_] if children.contains(tn)       => Nil
    case Some(tn: TreeNode[_]) if children.contains(tn) => Nil
    case Some(tn: TreeNode[_])                          => tn.argString :: Nil
    case tn: TreeNode[_]                                => tn.argString :: Nil
    case iter: Iterable[_] if iter.isEmpty              => Nil
    case seq: Seq[_]                                    => seq.mkString("[", ", ", "]") :: Nil
    case set: Set[_]                                    => set.toSeq.mkString("{", ", ", "}") :: Nil
    case array: Array[_] if array.isEmpty               => Nil
    case array: Array[_]                                => array.mkString("[", ", ", "]") :: Nil
    case null                                           => Nil
    case None                                           => Nil
    case Some(null)                                     => Nil
    case Some(any)                                      => any :: Nil
    case other                                          => other :: Nil
  }.mkString(", ")

  def argFilter: Any => Boolean = _ => true

  override def toString(): String = productPrefix
}

object TreeNode {

  case class RewriteRule[T <: TreeNode[_]](rule: PartialFunction[T, T])
    extends PartialFunction[T, T] {

    def apply(t: T): T = rule.apply(t)

    def isDefinedAt(t: T): Boolean = rule.isDefinedAt(t)
  }
}
