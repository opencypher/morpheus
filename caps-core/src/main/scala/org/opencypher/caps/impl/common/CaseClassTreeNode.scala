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

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import CaseClassTreeNode.mirror

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
      copyMethod(updatedConstructorParams: _*).asInstanceOf[T]
    }
  }

  private lazy val copyMethod = {
    val instanceMirror = mirror.reflect(self)
    val tpe = instanceMirror.symbol.asType.toType
    val copyMethodSymbol = tpe.decl(TermName("copy")).asMethod
    instanceMirror.reflectMethod(copyMethodSymbol)
  }

}

/**
  * Provides the shared runtime mirror for ```CaseClassTreeNode```.
  */
object CaseClassTreeNode {
  protected val mirror = universe.runtimeMirror(getClass.getClassLoader)
}
