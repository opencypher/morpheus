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
  override lazy val hashCode: Int = MurmurHash3.productHash(this)

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

  /**
    * Checks if the given tree is contained within that tree or is the tree itself.
    *
    * @param other other tree
    * @return true, iff `other` is contained in that tree
    */
  def containsTree(other: T): Boolean = {
    if (self == other) true else children.exists(_.containsTree(other))
  }

  /**
    * Applies the given [[org.opencypher.caps.impl.common.TreeNode.RewriteRule]] starting from the
    * leafs of that tree.
    *
    * @param rule rewrite rule
    * @return rewritten tree
    */
  def transformUp(rule: TreeNode.RewriteRule[T]): T = {
    val afterChildren = withNewChildren(children.map(_.transformUp(rule)))
    if (rule.isDefinedAt(afterChildren)) rule(afterChildren) else afterChildren
  }

  /**
    * Applies the given [[org.opencypher.caps.impl.common.TreeNode.RewriteRule]] starting from the
    * root of that tree.
    *
    * @note Note that the applied rule must not insert new parent nodes.
    *
    * @param rule rewrite rule
    * @return rewritten tree
    */
  def transformDown(rule: TreeNode.RewriteRule[T]): T = {
    val afterSelf = if (rule.isDefinedAt(self)) rule(self) else self
    afterSelf.withNewChildren(afterSelf.children.map(_.transformDown(rule)))
  }

  /**
    * Prints the tree node and its children recursively in a tree-style layout.
    *
    * @param depth indentation depth used by the recursive call
    * @return tree-style representation of that node and all (grand-)children
    */
  def pretty(depth: Int = 0): String = {

    def prefix(depth: Int): String = ("· " * depth) + "|-"

    val childrenString = children.foldLeft(new StringBuilder()) {
      case (agg, s) => agg.append(s.pretty(depth + 1))
    }
    s"${prefix(depth)}$self($argString)\n$childrenString"
  }

  /**
    * Concatenates all arguments of that tree node excluding the children.
    *
    * @return argument string
    */
  def argString: String = productIterator
    .filter(argFilter)
    .map {
      case tn: TreeNode[_] => tn.argString
      case other           => AsCode(other)
    }.mkString(", ")

  /**
    * Filters class arguments that must not be printed. Can be overridden by concrete tree nodes.
    *
    * @return filter for class arguments
    */
  def argFilter: Any => Boolean = {
    case tn: TreeNode[_] if children.contains(tn) => false
    case _ => true
  }

  override def toString(): String = getClass.getSimpleName
}

object TreeNode {

  case class RewriteRule[T <: TreeNode[_]](rule: PartialFunction[T, T])
    extends PartialFunction[T, T] {

    def apply(t: T): T = rule.apply(t)

    def isDefinedAt(t: T): Boolean = rule.isDefinedAt(t)
  }
}
