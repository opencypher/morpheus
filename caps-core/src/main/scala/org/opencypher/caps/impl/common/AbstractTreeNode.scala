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

import org.opencypher.caps.impl.spark.exception.Raise

import scala.reflect.ClassTag

/**
  * Class that implements the `children` and `withNewChildren` methods using reflection when implementing
  * `TreeNode` with a case class or case object.
  *
  * Requirements: All child nodes need to be individual constructor parameters and their order
  * in children is their order in the constructor. Every constructor parameter of type `T` is
  * assumed to be a child node.
  */
abstract class AbstractTreeNode[T <: TreeNode[T] : ClassTag] extends TreeNode[T] {
  self: T =>

  override lazy val children: Seq[T] = productIterator.toVector.collect { case t: T => t }

  /**
    * Cache children as a set for faster rewrites.
    */
  override lazy val childrenAsSet = children.toSet

  override def withNewChildren(newChildren: Seq[T]): T = {
    val newAsVector = newChildren.toVector
    if (children == newAsVector) {
      self
    } else {
      require(children.length == newAsVector.length,
        s"invalid children for $productPrefix: ${newAsVector.mkString(", ")}")
      val substitutions = children.toList.zip(newAsVector)
      val (updatedConstructorParams, _) = productIterator.foldLeft((Vector.empty[Any], substitutions)) {
        case ((result, remainingSubs), next) =>
          remainingSubs match {
            case (oldC, newC) :: tail if next == oldC => (result :+ newC, tail)
            case _ => (result :+ next, remainingSubs)
          }
      }
      val copyMethod = AbstractTreeNode.copyMethod(self)
      try {
        copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
      } catch {
        case e: Exception =>
          Raise.invalidArgument(s"valid constructor arguments for $productPrefix",
            s"""|${updatedConstructorParams.mkString(", ")}
                |Original exception: $e
                |Copy method: $copyMethod""".stripMargin)
      }
    }
  }

}

/**
  * Caches an instance of the copy method per case class type.
  */
object AbstractTreeNode {

  import java.util.concurrent.ConcurrentHashMap

  import collection.JavaConverters._
  import scala.reflect.runtime.universe
  import scala.reflect.runtime.universe._

  protected def copyMethod[T <: TreeNode[T]](instance: AbstractTreeNode[T]): MethodMirror = {
    val className = instance.getClass.getName
    cachedCopyMethods.getOrElse(className, {
      val instanceMirror = mirror.reflect(instance)
      val tpe = instanceMirror.symbol.asType.toType
      val copyMethodSymbol = tpe.decl(TermName("copy")).asMethod
      val copyMethod = instanceMirror.reflectMethod(copyMethodSymbol)
      cachedCopyMethods.put(className, copyMethod)
      copyMethod
    })
  }

  private val cachedCopyMethods = new ConcurrentHashMap[String, universe.MethodMirror].asScala

  protected val mirror = universe.runtimeMirror(getClass.getClassLoader)
}
