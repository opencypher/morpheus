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

/**
  * Class that implements the `children` and `withNewChildren` methods using reflection when implementing
  * `TreeNode` with a case class or case object.
  *
  * Requirements: All child nodes need to be individual constructor parameters and their order
  * in children is their order in the constructor. Every constructor parameter of type `T` is
  * assumed to be a child node.
  *
  * This class caches values that are expensive to recompute.
  *
  * The constructor can also contain a list of children, but there are constraints:
  *   - A list of children cannot be empty, because the current design relies on testing the type of an element.
  *   - If any children are contained in a list at all, then all list elements need to be children. This allows
  *     to only check the type of the first element.
  *   - There can be at most one list of children and there can be no normal child constructor parameters
  *     that appear after the list of children. This allows to call `withNewChildren` with a different number of
  *     children than the original node had and vary the length of the list to accommodate.
  */
abstract class AbstractTreeNode[T <: AbstractTreeNode[T]: ClassTag] extends TreeNode[T] {
  self: T =>

  override val children: Array[T] = {
    val constructorParamLength = productArity
    val childrenCount = {
      var count = 0
      var usedListOfChildren = false
      var i = 0
      while (i < constructorParamLength) {
        val pi = productElement(i)
        pi match {
          case _: T =>
            require(
              !usedListOfChildren,
              "there can be no normal child constructor parameters after a list of children.")
            count += 1
          case l: List[_] if l.nonEmpty =>
            // Need explicit pattern match for T, as `isInstanceOf` in `if` results in a warning.
            l.head match {
              case _: T =>
                require(!usedListOfChildren, "there can be at most one list of children in the constructor.")
                usedListOfChildren = true
                count += l.length
              case _ =>
            }
          case _ =>
        }
        i += 1
      }
      count
    }
    val childrenArray = new Array[T](childrenCount)
    if (childrenCount > 0) {
      var i = 0
      var ci = 0
      while (i < constructorParamLength) {
        val pi = productElement(i)
        pi match {
          case c: T =>
            childrenArray(ci) = c
            ci += 1
          case l: List[_] if l.nonEmpty =>
            // Need explicit pattern match for T, as `isInstanceOf` in `if` results in a warning.
            l.head match {
              case _: T =>
                val j = l.iterator
                while (j.hasNext) {
                  val child = j.next
                  try {
                    childrenArray(ci) = child.asInstanceOf[T]
                  } catch {
                    case c: ClassCastException =>
                      throw InvalidConstructorArgument(
                        s"""Expected a list that contains either no children or only children
                           |but found a mixed list that contains a child as the head element,
                           |but also one with a non-child type: ${c.getMessage}.
                           |""".stripMargin
                      )
                  }
                  ci += 1
                }
              case _ =>
            }
          case _ =>
        }
        i += 1
      }
    }
    childrenArray
  }

  @inline override def withNewChildren(newChildren: Array[T]): T = {
    if (sameAsCurrentChildren(newChildren)) {
      self
    } else {
      val updatedConstructorParams = updateConstructorParams(newChildren)
      val copyMethod = AbstractTreeNode.copyMethod(self)
      try {
        copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
      } catch {
        case e: Exception =>
          throw InvalidConstructorArgument(
            s"""Expected valid constructor arguments for $productPrefix
               |but found ${updatedConstructorParams.mkString(", ")}.
               |""".stripMargin,
            Some(e)
          )
      }
    }
  }

  final override lazy val hashCode: Int = super.hashCode

  final override lazy val size: Int = super.size

  final override lazy val height: Int = super.height

  final lazy val childrenAsSet = children.toSet

  @inline final override def containsChild(other: T): Boolean = {
    childrenAsSet.contains(other)
  }

  @inline final override def map[O <: TreeNode[O]: ClassTag](f: T => O): O = super.map(f)

  @inline final override def foreach[O](f: T => O): Unit = super.foreach(f)

  @inline final override def containsTree(other: T): Boolean = super.containsTree(other)

  @inline private final def updateConstructorParams(newChildren: Array[T]): Array[Any] = {
    val parameterArrayLength = productArity
    val childrenLength = children.length
    val newChildrenLength = newChildren.length
    val parameterArray = new Array[Any](parameterArrayLength)
    var productIndex = 0
    var childrenIndex = 0
    while (productIndex < parameterArrayLength) {
      val currentProductElement = productElement(productIndex)
      def nonChildCase(): Unit = {
        parameterArray(productIndex) = currentProductElement
      }
      currentProductElement match {
        case c: T if childrenIndex < childrenLength && c == children(childrenIndex) =>
          parameterArray(productIndex) = newChildren(childrenIndex)
          childrenIndex += 1
        case l: List[_] if childrenIndex < childrenLength && l.nonEmpty =>
          // Need explicit pattern match for T, as `isInstanceOf` in `if` results in a warning.
          l.head match {
            case _: T =>
              require(newChildrenLength > childrenIndex, s"a list of children cannot be empty.")
              parameterArray(productIndex) = newChildren.slice(childrenIndex, newChildrenLength).toList
              childrenIndex = newChildrenLength
            case _ => nonChildCase
          }
        case _ => nonChildCase
      }
      productIndex += 1
    }
    require(
      childrenIndex == newChildrenLength,
      "invalid number of children or used an empty list of children in the original node.")
    parameterArray
  }

  @inline private final def sameAsCurrentChildren(newChildren: Array[T]): Boolean = {
    val childrenLength = children.length
    if (childrenLength != newChildren.length) {
      false
    } else {
      var i = 0
      while (i < childrenLength && children(i) == newChildren(i)) i += 1
      i == childrenLength
    }
  }

}

/**
  * Caches an instance of the copy method per case class type.
  */
object AbstractTreeNode {

  import scala.reflect.runtime.universe
  import scala.reflect.runtime.universe._

  // No synchronization required: No problem if a cache entry is lost due to a concurrent write.
  @volatile private var cachedCopyMethods = Map.empty[Class[_], MethodMirror]

  private final lazy val mirror = universe.runtimeMirror(getClass.getClassLoader)

  @inline protected final def copyMethod(instance: AbstractTreeNode[_]): MethodMirror = {
    val instanceClass = instance.getClass
    cachedCopyMethods.getOrElse(
      instanceClass, {
        val copyMethod = reflectCopyMethod(instance)
        cachedCopyMethods = cachedCopyMethods.updated(instanceClass, copyMethod)
        copyMethod
      }
    )
  }

  @inline private final def reflectCopyMethod(instance: Object): MethodMirror = {
    val instanceMirror = mirror.reflect(instance)
    val tpe = instanceMirror.symbol.asType.toType
    val copyMethodSymbol = tpe.decl(TermName("copy")).asMethod
    instanceMirror.reflectMethod(copyMethodSymbol)
  }

}

case class InvalidConstructorArgument(message: String, originalException: Option[Exception] = None)
    extends RuntimeException(message, originalException.orNull)
