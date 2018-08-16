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

import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf, typeTag}

/**
  * Class that implements the `children` and `withNewChildren` methods using reflection when implementing
  * `TreeNode` with a case class or case object.
  *
  * This class caches values that are expensive to recompute.
  *
  * The constructor can also contain [[NonEmptyList]]s, [[List]]s, and [[Option]]s that contain children.
  * This works as long as there is an unambiguous way to serialize those children to the `children` array,
  * as well as an unambiguous way to assign the children in `withNewChildren` to the different constructor parameters.
  *
  * It is possible to override the defaults and use custom `children`/`withNewChildren` implementations.
  */
abstract class AbstractTreeNode[T <: AbstractTreeNode[T] : TypeTag] extends TreeNode[T] {
  self: T =>

  override protected def tt: TypeTag[T] = implicitly[TypeTag[T]]

  override implicit protected def ct: ClassTag[T] = ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))

  override val children: Array[T] = {
    if (productIterator.isEmpty) {
      Array.empty[T]
    } else {
      val copyMethod = AbstractTreeNode.copyMethod(self)
      lazy val treeType = typeOf[T].erasure
      lazy val paramTypes: Seq[Type] = copyMethod.symbol.paramLists.head.map(_.typeSignature).toIndexedSeq
      productIterator.toArray.zipWithIndex.flatMap {
        case (t: T, _) => Some(t)
        case (o: Option[_], i) if paramTypes(i).typeArgs.head <:< treeType => o.asInstanceOf[Option[T]]
        case (l: List[_], i) if paramTypes(i).typeArgs.head <:< treeType => l.asInstanceOf[List[T]]
        case (nel: NonEmptyList[_], i) if paramTypes(i).typeArgs.head <:< treeType => nel.toList.asInstanceOf[List[T]]
        case _ => Nil
      }
    }
  }

  @inline override def withNewChildren(newChildren: Array[T]): T = {
    if (sameAsCurrentChildren(newChildren)) {
      self
    } else {
      val copyMethod = AbstractTreeNode.copyMethod(self)
      val copyMethodParamTypes = copyMethod.symbol.paramLists.flatten.zipWithIndex
      val valueAndTypeTuples = copyMethodParamTypes.map { case (param, index) =>
        val value = if (index < productArity) {
          // Access product element to retrieve the value
          productElement(index)
        } else {
          tt // Workaround to get implicit tag without reflection
        }
        value -> param.typeSignature
      }
      val updatedConstructorParams = updateConstructorParams(newChildren, valueAndTypeTuples)
      try {
        copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
      } catch {
        case e: Exception => throw InvalidConstructorArgument(
          s"""|Expected valid constructor arguments for $productPrefix
              |Old children: ${children.mkString(", ")}
              |New children: ${newChildren.mkString(", ")}
              |Current product: ${productIterator.mkString(", ")}
              |Constructor arguments updated with new children: ${updatedConstructorParams.mkString(", ")}.""".stripMargin, Some(e))
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

  @inline final override def map[O <: TreeNode[O] : ClassTag](f: T => O): O = super.map(f)

  @inline final override def foreach[O](f: T => O): Unit = super.foreach(f)

  @inline final override def containsTree(other: T): Boolean = super.containsTree(other)

  @inline private final def updateConstructorParams(
    newChildren: Array[T],
    currentValuesAndTypes: List[(Any, Type)]
  ): Array[Any] = {
    // Returns true iff `instance` could be an element of List/NonEmptyList/Option container type `tpe`
    def couldBeElementOf(instance: Any, tpe: Type): Boolean = {
      currentMirror.reflect(instance).symbol.toType <:< tpe.typeArgs.head
    }

    val (unassignedChildren, constructorParams) = currentValuesAndTypes.foldLeft(newChildren.toList -> Vector.empty[Any]) {
      case ((remainingChildren, currentConstructorParams), nextValueAndType) =>
        nextValueAndType match {
          case (_: T, _) =>
            remainingChildren.tail -> (currentConstructorParams :+ remainingChildren.head)
          case (_: Option[_], tpe) if tpe.typeArgs.head <:< typeOf[T] =>
            val option: Option[T] = remainingChildren.headOption.filter { c => couldBeElementOf(c, tpe) }
            remainingChildren.drop(option.size) -> (currentConstructorParams :+ option)
          case (_: List[_], tpe) if tpe.typeArgs.head <:< typeOf[T] =>
            val childrenList: List[T] = remainingChildren.takeWhile { c => couldBeElementOf(c, tpe) }
            remainingChildren.drop(childrenList.size) -> (currentConstructorParams :+ childrenList)
          case (_: NonEmptyList[_], tpe) if tpe.typeArgs.head <:< typeOf[T] =>
            val childrenList = NonEmptyList.fromListUnsafe(remainingChildren.takeWhile { c => couldBeElementOf(c, tpe) })
            remainingChildren.drop(childrenList.size) -> (currentConstructorParams :+ childrenList)
          case (value, _) =>
            remainingChildren -> (currentConstructorParams :+ value)
        }
    }

    assert(unassignedChildren.isEmpty,
      s"Could not assign nodes [${unassignedChildren.mkString(", ")}] to parameters of ${getClass.getSimpleName}")
    constructorParams.toArray
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
    try {
      val instanceMirror = mirror.reflect(instance)
      val tpe = instanceMirror.symbol.asType.toType
      val copyMethodSymbol = tpe.decl(TermName("copy")).asMethod
      instanceMirror.reflectMethod(copyMethodSymbol)
    } catch {
      case e: Exception => throw new UnsupportedOperationException(
        s"Could not reflect the copy method of ${instance.toString.filterNot(_ == '$')}", e)
    }
  }

}

case class InvalidConstructorArgument(message: String, originalException: Option[Exception] = None)
  extends RuntimeException(message, originalException.orNull)
