/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.trees

import scala.reflect.ClassTag


abstract class TreeTransformer[I <: TreeNode[I] : ClassTag, O] {
  def rewrite(tree: I): O
}

abstract class TreeRewriter[T <: TreeNode[T] : ClassTag] extends TreeTransformer[T, T]

/**
  * Applies the given partial function starting from the leafs of this tree.
  */
case class BottomUp[T <: TreeNode[T] : ClassTag](rule: PartialFunction[T, T]) extends TreeRewriter[T] {

  def rewrite(tree: T): T = {
    val childrenLength = tree.children.length
    val afterChildren = if (childrenLength == 0) {
      tree
    } else {
      val updatedChildren = {
        val childrenCopy = new Array[T](childrenLength)
        var i = 0
        while (i < childrenLength) {
          childrenCopy(i) = rewrite(tree.children(i))
          i += 1
        }
        childrenCopy
      }
      tree.withNewChildren(updatedChildren)
    }
    if (rule.isDefinedAt(afterChildren)) rule(afterChildren) else afterChildren
  }

}

/**
  * Applies the given partial function starting from the root of this tree.
  *
  * @note Note the applied rule cannot insert new parent nodes.
  */
case class TopDown[T <: TreeNode[T] : ClassTag](rule: PartialFunction[T, T]) extends TreeRewriter[T] {

  def rewrite(tree: T): T = {
    val afterSelf = if (rule.isDefinedAt(tree)) rule(tree) else tree
    val childrenLength = afterSelf.children.length
    if (childrenLength == 0) {
      afterSelf
    } else {
      val updatedChildren = {
        val childrenCopy = new Array[T](childrenLength)
        var i = 0
        while (i < childrenLength) {
          childrenCopy(i) = rewrite(afterSelf.children(i))
          i += 1
        }
        childrenCopy
      }
      afterSelf.withNewChildren(updatedChildren)
    }
  }

}

/**
  * Applies the given transformation starting from the leafs of this tree.
  */
case class Transform[I <: TreeNode[I] : ClassTag, O](
  transform: (I, List[O]) => O
) extends TreeTransformer[I, O] {

  def rewrite(tree: I): O = {
    val children = tree.children
    val childrenLength = children.length
    if (childrenLength == 0) {
      transform(tree, List.empty[O])
    } else {
      val transformedChildren = {
        val tmpChildren = new Array[O](childrenLength)
        var i = 0
        while (i < childrenLength) {
          tmpChildren(i) = rewrite(children(i))
          i += 1
        }
        tmpChildren.toList
      }
      transform(tree, transformedChildren)
    }
  }

}
