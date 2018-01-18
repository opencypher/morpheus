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
package org.opencypher.caps.trees

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
  * This is the basic tree node class. Usually it makes more sense to use `AbstractTreeNode`, which uses reflection
  * to generate the `children` and `withNewChildren` field/method. Our benchmarks show that manually implementing
  * them can result in a speedup of a factor of ~3 for tree rewrites, so in performance critical places it might
  * make sense to extend `TreeNode` instead.
  *
  * This class uses array operations instead of Scala collections, both for improved performance as well as to save
  * stack frames during recursion, which allows it to operate on trees that are several thousand nodes high.
  */
abstract class TreeNode[T <: TreeNode[T]: ClassTag] extends Product with Traversable[T] {
  self: T =>

  def withNewChildren(newChildren: Array[T]): T

  def children: Array[T] = Array.empty

  override def hashCode: Int = MurmurHash3.productHash(self)

  def arity: Int = children.length

  def isLeaf: Boolean = height == 1

  def height: Int = {
    val childrenLength = children.length
    var i = 0
    var result = 0
    while (i < childrenLength) {
      result = math.max(result, children(i).height)
      i += 1
    }
    result + 1
  }

  override def size: Int = {
    val childrenLength = children.length
    var i = 0
    var result = 1
    while (i < childrenLength) {
      result += children(i).size
      i += 1
    }
    result
  }

  def map[O <: TreeNode[O]: ClassTag](f: T => O): O = {
    val childrenLength = children.length
    if (childrenLength == 0) {
      f(self)
    } else {
      val mappedChildren = new Array[O](childrenLength)
      var i = 0
      while (i < childrenLength) {
        mappedChildren(i) = f(children(i))
        i += 1
      }
      f(self).withNewChildren(mappedChildren)
    }
  }

  override def foreach[O](f: T => O): Unit = {
    f(this)
    val childrenLength = children.length
    var i = 0
    while (i < childrenLength) {
      children(i).foreach(f)
      i += 1
    }
  }

  /**
    * Checks if the parameter tree is contained within this tree. A tree always contains itself.
    *
    * @param other other tree
    * @return true, iff `other` is contained in that tree
    */
  def containsTree(other: T): Boolean = {
    if (self == other) {
      true
    } else {
      val childrenLength = children.length
      var i = 0
      while (i < childrenLength && !children(i).containsTree(other)) i += 1
      i != childrenLength
    }
  }

  /**
    * Checks if `other` is a direct child of this tree.
    *
    * @param other other tree
    * @return true, iff `other` is a direct child of this tree
    */
  def containsChild(other: T): Boolean = {
    children.contains(other)
  }

  /**
    * Prints the tree node and its children recursively in a tree-style layout.
    *
    * @param depth indentation depth used by the recursive call
    * @return tree-style representation of that node and all (grand-)children
    */
  def pretty(implicit depth: Int = 0): String = {

    def prefix(depth: Int): String = ("· " * depth) + "|-"

    val childrenString = children.foldLeft(new StringBuilder()) {
      case (agg, s) => agg.append(s.pretty(depth + 1))
    }
    s"${prefix(depth)}$self\n$childrenString"
  }

  /**
    * Turns all arguments in `args` into a string that describes the arguments.
    *
    * @return argument string
    */
  def argString: String = args.mkString(", ")

  /**
    * Arguments that should be printed. The default implementation excludes children.
    */
  def args: Iterator[Any] = {
    def generalCase(arg: Any) = Some(arg.toString)
    productIterator.flatMap {
      case c: T if containsChild(c)     => None // Don't print children
      case i: Iterable[_] if i.nonEmpty =>
        // Need explicit pattern match for T, as `isInstanceOf` in `if` results in a warning.
        i.head match {
          case _: T => None // Don't print children
          case _    => generalCase(i)
        }
      case other => generalCase(other)
    }
  }

  override def toString = s"${getClass.getSimpleName}${if (argString.isEmpty) "" else s"($argString)"}"
}
