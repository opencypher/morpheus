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

import org.opencypher.caps.impl.exception.Raise

import scala.reflect.ClassTag

/**
  * Class that implements the `children` and `withNewChildren` methods using reflection when implementing
  * `TreeNode` with a case class or case object.
  *
  * Requirements: All child nodes need to be individual constructor parameters and their order
  * in children is their order in the constructor. Every constructor parameter of type `T` is
  * assumed to be a child node.
  *
  * This class caches values that are expensive to recompute. It also uses array operations instead of
  * Scala collections, both for improved performance as well as to save stack frames, which allows to operate on
  * trees that are several thousand nodes high.
  */
abstract class AbstractTreeNode[T <: AbstractTreeNode[T]: ClassTag] extends TreeNode[T] {
  self: T =>

  override final val children: Array[T] = {
    val constructorParamLength = productArity
    val childrenArray = new Array[T](constructorParamLength)
    var i = 0
    var ci = 0
    while (i < constructorParamLength) {
      val pi = productElement(i)
      pi match {
        case c: T =>
          childrenArray(ci) = c
          ci += 1
        case _ =>
      }
      i += 1
    }
    val properSizedChildren = new Array[T](ci)
    System.arraycopy(childrenArray, 0, properSizedChildren, 0, ci)
    properSizedChildren
  }

  @inline override final def withNewChildren(newChildren: Array[T]): T = {
    if (sameAsCurrentChildren(newChildren)) {
      self
    } else {
      val updatedConstructorParams = updateConstructorParams(newChildren)
      val copyMethod = AbstractTreeNode.copyMethod(self)
      try {
        copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
      } catch {
        case e: Exception =>
          Raise.invalidArgument(
            s"valid constructor arguments for $productPrefix",
            s"""|${updatedConstructorParams.mkString(", ")}
                |Original exception: $e
                |Copy method: $copyMethod""".stripMargin
          )
      }
    }
  }

  override final lazy val hashCode: Int = super.hashCode

  final lazy val childrenAsSet = children.toSet

  override final lazy val size: Int = {
    val childrenLength = children.length
    var i = 0
    var result = 1
    while (i < childrenLength) {
      result += children(i).size
      i += 1
    }
    result
  }

  final override lazy val height: Int = {
    val childrenLength = children.length
    var i = 0
    var result = 0
    while (i < childrenLength) {
      result = math.max(result, children(i).height)
      i += 1
    }
    result + 1
  }

  @inline final override def map[O <: TreeNode[O]: ClassTag](f: T => O): O = {
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

  @inline final override def foreach[O](f: T => O): Unit = {
    f(this)
    val childrenLength = children.length
    var i = 0
    while (i < childrenLength) {
      children(i).foreach(f)
      i += 1
    }
  }

  @inline final override def containsTree(other: T): Boolean = {
    if (self == other) {
      true
    } else {
      val childrenLength = children.length
      var i = 0
      while (i < childrenLength && !children(i).containsTree(other)) i += 1
      i != childrenLength
    }
  }

  @inline final override def containsChild(other: T): Boolean = {
    childrenAsSet.contains(other)
  }

  @inline final override def transformUp(rule: PartialFunction[T, T]): T = {
    val childrenLength = children.length
    val afterChildren = if (childrenLength == 0) {
      self
    } else {
      val updatedChildren = {
        val childrenCopy = new Array[T](childrenLength)
        var i = 0
        while (i < childrenLength) {
          childrenCopy(i) = children(i).transformUp(rule)
          i += 1
        }
        childrenCopy
      }
      withNewChildren(updatedChildren)
    }
    if (rule.isDefinedAt(afterChildren)) rule(afterChildren) else afterChildren
  }

  @inline final override def transformDown(rule: PartialFunction[T, T]): T = {
    val afterSelf = if (rule.isDefinedAt(self)) rule(self) else self
    val childrenLength = afterSelf.children.length
    if (childrenLength == 0) {
      afterSelf
    } else {
      val updatedChildren = {
        val childrenCopy = new Array[T](childrenLength)
        var i = 0
        while (i < childrenLength) {
          childrenCopy(i) = afterSelf.children(i).transformDown(rule)
          i += 1
        }
        childrenCopy
      }
      afterSelf.withNewChildren(updatedChildren)
    }
  }

  @inline private final def updateConstructorParams(newChildren: Array[T]): Array[Any] = {
    require(
      children.length == newChildren.length,
      s"invalid children for $productPrefix: ${newChildren.mkString(", ")}")
    val parameterArrayLength = productArity
    val childrenLength = children.length
    val parameterArray = new Array[Any](parameterArrayLength)
    var productIndex = 0
    var childrenIndex = 0
    while (productIndex < parameterArrayLength) {
      val currentProductElement = productElement(productIndex)
      if (childrenIndex < childrenLength && currentProductElement == children(childrenIndex)) {
        parameterArray(productIndex) = newChildren(childrenIndex)
        childrenIndex += 1
      } else {
        parameterArray(productIndex) = currentProductElement
      }
      productIndex += 1
    }
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
