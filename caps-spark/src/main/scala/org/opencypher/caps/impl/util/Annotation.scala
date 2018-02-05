/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.util

import org.opencypher.caps.api.schema.{Labels, Node, Relationship, RelationshipType}

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

private[caps] object Annotation {
  def labels[E <: Node: TypeTag]: Set[String] = {
    get[Labels, E] match {
      case Some(ls) => ls.labels.toSet
      case None     => Set(runtimeClass[E].getSimpleName)
    }
  }

  def relType[E <: Relationship: TypeTag]: String = {
    get[RelationshipType, E] match {
      case Some(RelationshipType(tpe)) => tpe
      case None                        => runtimeClass[E].getSimpleName.toUpperCase
    }
  }

  def get[A <: StaticAnnotation: TypeTag, E: TypeTag]: Option[A] = {
    val maybeAnnotation = staticClass[E].annotations.find(_.tree.tpe =:= typeOf[A])
    maybeAnnotation.map { annotation =>
      val tb = typeTag[E].mirror.mkToolBox()
      val instance = tb.eval(tb.untypecheck(annotation.tree)).asInstanceOf[A]
      instance
    }
  }

  private def runtimeClass[E: TypeTag]: Class[E] = {
    val tag = typeTag[E]
    val mirror = tag.mirror
    val runtimeClass = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    runtimeClass.asInstanceOf[Class[E]]
  }

  private def staticClass[E: TypeTag]: ClassSymbol = {
    val mirror = typeTag[E].mirror
    mirror.staticClass(runtimeClass[E].getCanonicalName)
  }
}
