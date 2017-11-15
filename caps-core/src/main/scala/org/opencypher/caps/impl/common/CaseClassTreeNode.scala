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

import scala.reflect.ClassTag

/**
  * Class that implements the ```children``` and ```withNewChildren``` methods using reflection when implementing
  * ```TreeNode``` with a case class.
  *
  * Requirements: All child nodes need to be individual constructor parameters and their order
  * in children is their oder in the constructor. Every constructor parameter of type ```T``` is
  * assumed to be a child node.
  */
abstract class CaseClassTreeNode[T <: TreeNode[T] : ClassTag] extends TreeNode[T] {
  self: T =>

  override lazy val children: Seq[T] = productIterator.toSeq.collect { case t: T => t }

  override def withNewChildren(newChildren: Seq[T]): T = {
    if (children == newChildren) {
      self
    } else {
      require(children.length == newChildren.length)
      val substitutions = children.toList.zip(newChildren)
      val (updatedConstructorParams, _) = productIterator.foldLeft((Vector.empty[Any], substitutions)) {
        case ((result, remainingSubs), next) =>
          remainingSubs match {
            case (oldC, newC) :: tail if next == oldC =>  (result :+ newC, tail)
            case _ => (result :+ next, remainingSubs)
          }
      }
      val copyMethod = CaseClassTreeNode.copyMethod(productPrefix, self)
      copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
    }
  }

  /**
    * Cache class name as product prefix for fast retrieval of the copy method.
    */
  override lazy final val productPrefix: String = getClass.getSimpleName

}

/**
  * Provides the shared runtime mirror for ```CaseClassTreeNode``` and caches an instance of the copy method per
  * case class tree node.
  */
object CaseClassTreeNode {
  import java.util.concurrent.ConcurrentHashMap

  import collection.JavaConverters._
  import scala.reflect.runtime.universe
  import scala.reflect.runtime.universe._

  protected def copyMethod[T <: TreeNode[T]](simpleName: String, instance: CaseClassTreeNode[T]): MethodMirror = {
    cachedCopyMethods.getOrElse(simpleName, {
      val instanceMirror = mirror.reflect(instance)
      val tpe = instanceMirror.symbol.asType.toType
      val copyMethodSymbol = tpe.decl(TermName("copy")).asMethod
      val copyMethod = instanceMirror.reflectMethod(copyMethodSymbol)
      cachedCopyMethods.put(simpleName, copyMethod)
      copyMethod
    })
  }

  private val cachedCopyMethods = new ConcurrentHashMap[String, universe.MethodMirror].asScala

  protected val mirror = universe.runtimeMirror(getClass.getClassLoader)
}
