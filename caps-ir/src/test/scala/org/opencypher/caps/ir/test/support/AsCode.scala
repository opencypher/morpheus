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
package org.opencypher.caps.ir.test.support

import org.opencypher.caps.api.types.CypherType

/**
  * Returns a string that can be pasted as an object definition for standard case classes,
  * some other products, collections and objects.
  */
// TODO: remove once MatchHelper can use scalatest again
object AsCode {

  implicit class ImplicitAsCode(a: Any) {
    def asCode(implicit specialMappings: PartialFunction[Any, String] = Map.empty): String = {
      anyAsCode(a)(specialMappings)
    }
  }

  def apply(a: Any): String = {
    anyAsCode(a)(Map.empty[Any, String])
  }

  def apply(a: Any, specialMappings: Map[Any, String]): String = {
    anyAsCode(a)(specialMappings)
  }

  private def anyAsCode(a: Any)(implicit specialMappings: PartialFunction[Any, String] = Map.empty): String = {
    if (specialMappings.isDefinedAt(a)) specialMappings(a)
    else {
      a match {
        case null         => "null"
        case s: String    => s""""$s""""
        case p: Product   => productAsCode(p)
        case t: Seq[_]    => traversableAsCode(t)
        case t: Set[_]    => traversableAsCode(t)
        case t: Map[_, _] => traversableAsCode(t)
        case b: Boolean   => b.toString
        case i: Int       => i.toString
        case l: Long      => l.toString
        case f: Float     => f.toString
        case d: Double    => d.toString
        // Other objects are represented with their class name in lower case
        case other => s"${other.getClass.getSimpleName.toLowerCase}"
      }
    }
  }

  private def traversableAsCode(t: Traversable[_])(
      implicit specialMappings: PartialFunction[Any, String] = Map.empty): String = {
    if (specialMappings.isDefinedAt(t)) specialMappings(t)
    else {
      val elementString = t.map(anyAsCode(_)).mkString(", ")
      val simpleName = t.getClass.getSimpleName
      if (simpleName.endsWith("$")) {
        t.toString
      } else {
        val normalStringRepresentation = t.toString
        val indexOfOpeningParenthesis = normalStringRepresentation.indexOf("(")
        if (indexOfOpeningParenthesis != -1) {
          s"${normalStringRepresentation.substring(0, indexOfOpeningParenthesis)}($elementString)"
        } else {
          t.toString.substring(indexOfOpeningParenthesis)
        }
      }
    }
  }

  private def productAsCode(p: Product)(implicit specialMappings: PartialFunction[Any, String] = Map.empty): String = {
    if (specialMappings.isDefinedAt(p)) specialMappings(p)
    else {
      if (p.productIterator.isEmpty) {
        if (p.isInstanceOf[CypherType]) { // Special case for cypher type
          val name = p.getClass.getSimpleName
          if (name.endsWith("$")) name.dropRight(1) else name
        } else {
          p.toString
        }
      } else {
        s"${p.getClass.getSimpleName}(${p.productIterator.map(anyAsCode(_)(specialMappings)).mkString(", ")})"
      }
    }
  }

}
