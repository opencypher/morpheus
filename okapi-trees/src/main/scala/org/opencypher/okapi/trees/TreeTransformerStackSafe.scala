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

import cats.data.NonEmptyList

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
  * Common trait of all classes that represent tree operations for off-stack rewrites.
  */
sealed trait TreeOperation[T <: TreeNode[T], O]

/**
  * Represents a child-rewrite operation during off-stack transformations.
  */
case class RewriteChildren[I <: TreeNode[I], O](
  node: I,
  rewrittenChildren: List[O] = List.empty[O]
) extends TreeOperation[I, O]

/**
  * Represents a node-rewrite operation during off-stack transformations.
  */
case class RewriteNode[I <: TreeNode[I], O](
  node: I,
  rewrittenChildren: List[O] = List.empty[O]
) extends TreeOperation[I, O]

/**
  * Represents a finished rewrite during off-stack transformations.
  */
case class Done[I <: TreeNode[I], O](rewrittenChildren: List[O]) extends TreeOperation[I, O]

/**
  * This is the base-class for stack-safe tree transformations.
  */
trait TransformerStackSafe[I <: TreeNode[I], O] extends TreeTransformer[I, O] {

  type NonEmptyStack = NonEmptyList[TreeOperation[I, O]]

  type Stack = List[TreeOperation[I, O]]

  implicit class StackOps(val stack: Stack) {

    @inline final def push(op: TreeOperation[I, O]): NonEmptyStack = {
      NonEmptyList(op, stack)
    }

  }

  def Stack(op: TreeOperation[I, O]): NonEmptyStack = NonEmptyList.one(op)

  implicit class NonEmptyStackOps(val stack: NonEmptyStack) {

    @inline final def push(op: TreeOperation[I, O]): NonEmptyStack = {
      op :: stack
    }

  }

  /**
    * Called on each node when going down the tree.
    */
  def rewriteChildren(
    node: I,
    rewrittenChildren: List[O],
    stack: Stack
  ): NonEmptyStack

  /**
    * Called on each node when going up the tree.
    */
  def rewriteNode(
    node: I,
    rewrittenChildren: List[O],
    stack: Stack
  ): NonEmptyStack

  @tailrec
  protected final def run(stack: NonEmptyList[TreeOperation[I, O]]): O = stack match {
    case NonEmptyList(RewriteChildren(node, rewrittenChildren), tail) => run(rewriteChildren(node, rewrittenChildren, tail))
    case NonEmptyList(RewriteNode(node, rewrittenChildren), tail) => run(rewriteNode(node, rewrittenChildren, tail))
    case NonEmptyList(Done(rewritten), tail) =>
      tail match {
        case Nil => rewritten match {
          case result :: Nil => result
          case invalid => throw new IllegalStateException(s"Invalid rewrite produced $invalid instead of a single final value.")
        }
        case Done(nextNodes) :: nextTail => run(nextTail.push(Done(rewritten ::: nextNodes)))
        case RewriteChildren(nextNode, rewrittenChildren) :: nextTail => run(nextTail.push(RewriteChildren(nextNode, rewritten ::: rewrittenChildren)))
        case RewriteNode(nextNode, rewrittenChildren) :: nextTail => run(nextTail.push(RewriteNode(nextNode, rewritten ::: rewrittenChildren)))
      }
  }

  @inline final override def rewrite(tree: I): O = run(Stack(RewriteChildren(tree)))

}

/**
  * Common parent of [[BottomUpStackSafe]] and [[TopDownStackSafe]]
  */
trait SameTypeTransformerStackSafe[T <: TreeNode[T]] extends TransformerStackSafe[T, T] {

  protected val partial: PartialFunction[T, T]

  @inline final def rule: T => T = partial.orElse(PartialFunction(identity[T]))

}

/**
  * Applies the given partial function starting from the leafs of this tree.
  *
  * @note This is a stack-safe version of [[BottomUp]].
  */
case class BottomUpStackSafe[T <: TreeNode[T] : ClassTag](
  partial: PartialFunction[T, T]
) extends SameTypeTransformerStackSafe[T] {

  @inline final override def rewriteChildren(node: T, rewrittenChildren: List[T], stack: Stack): NonEmptyStack = {
    if (node.children.isEmpty) {
      stack.push(Done(rule(node) :: rewrittenChildren))
    } else {
      node.children.foldLeft(stack.push(RewriteNode(node, rewrittenChildren))) { case (currentStack, child) =>
        currentStack.push(RewriteChildren(child))
      }
    }
  }

  @inline final override def rewriteNode(node: T, rewrittenChildren: List[T], stack: Stack): NonEmptyStack = {
    val (currentRewrittenChildren, nextRewrittenChildren) = rewrittenChildren.splitAt(node.children.length)
    val nodeWithUpdatedChildren = rule(node.withNewChildren(currentRewrittenChildren.toArray))
    stack.push(Done(nodeWithUpdatedChildren :: nextRewrittenChildren))
  }
}

/**
  * Applies the given partial function starting from the root of this tree.
  *
  * @note Note the applied rule cannot insert new parent nodes.
  * @note This is a stack-safe version of [[TopDown]].
  */
case class TopDownStackSafe[T <: TreeNode[T] : ClassTag](
  partial: PartialFunction[T, T]
) extends SameTypeTransformerStackSafe[T] {

  @inline final override def rewriteChildren(node: T, rewrittenChildren: List[T], stack: Stack): NonEmptyStack = {
    val updatedNode = rule(node)
    if (updatedNode.children.isEmpty) {
      stack.push(Done(updatedNode :: rewrittenChildren))
    } else {
      updatedNode.children.foldLeft(stack.push(RewriteNode(updatedNode, rewrittenChildren))) {
        case (currentStack, child) =>
          currentStack.push(RewriteChildren(child))
      }
    }
  }

  @inline final override def rewriteNode(node: T, rewrittenChildren: List[T], stack: Stack): NonEmptyStack = {
    val (currentRewrittenChildren, nextRewrittenChildren) = rewrittenChildren.splitAt(node.children.length)
    val nodeWithUpdatedChildren = node.withNewChildren(currentRewrittenChildren.toArray)
    stack.push(Done(nodeWithUpdatedChildren :: nextRewrittenChildren))
  }

}

/**
  * Applies the given transformation starting from the leaves of this tree.
  *
  * @note This is a stack-safe version of [[Transform]].
  */
case class TransformStackSafe[I <: TreeNode[I] : ClassTag, O](
  transform: (I, List[O]) => O
) extends TransformerStackSafe[I, O] {

  @inline final override def rewriteChildren(node: I, rewrittenChildren: List[O], stack: Stack): NonEmptyStack = {
    if (node.children.isEmpty) {
      stack.push(Done(transform(node, List.empty[O]) :: rewrittenChildren))
    } else {
      node.children.foldLeft(stack.push(RewriteNode(node, rewrittenChildren))) { case (currentStack, child) =>
        currentStack.push(RewriteChildren(child))
      }
    }
  }

  @inline final override def rewriteNode(node: I, rewrittenChildren: List[O], stack: Stack): NonEmptyStack = {
    val (currentRewrittenChildren, nextRewrittenChildren) = rewrittenChildren.splitAt(node.children.length)
    val transformedNode = transform(node, currentRewrittenChildren)
    stack.push(Done(transformedNode :: nextRewrittenChildren))
  }

}
