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
  * Adds quotes to strings so outputs can be pasted as object definitions.
  */
object AsCode extends (Any => String) {

  def apply(a: Any): String = {
    anyAsCode(a)(Map.empty[Any, String])
  }

  def apply(a: Any, specialMappings: Map[Any, String]): String = {
    anyAsCode(a)(specialMappings)
  }

  private def anyAsCode(a: Any)(implicit specialMappings: Map[Any, String] = Map.empty): String = {
    if (specialMappings.isDefinedAt(a)) specialMappings(a)
    else {
      a match {
        case null         => "null"
        case s: String    => s""""$s""""
        case p: Product   => productAsCode(p)
        case t: Seq[_]    => traversableAsCode(t)
        case t: Set[_]    => traversableAsCode(t)
        case t: Map[_, _] => traversableAsCode(t)
        case other =>
          if (!other.isInstanceOf[AnyRef]) {
            other.toString // for primitives
          } else {
            s"${other.getClass.getSimpleName}"
//            Raise.notYetImplemented(
//              s"constructor code for value $other of class ${other.getClass.getSimpleName}"
//            )
          }
      }
    }
  }

  private def traversableAsCode(t: Traversable[_])(implicit specialMappings: Map[Any, String] = Map.empty): String = {
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

  private def productAsCode(p: Product)(implicit specialMappings: Map[Any, String] = Map.empty): String = {
    if (specialMappings.isDefinedAt(p)) specialMappings(p)
    else {
      if (p.productIterator.isEmpty) {
        p.toString
      } else {
        s"${p.getClass.getSimpleName}(${p.productIterator.map(anyAsCode(_)).mkString(", ")})"
      }
    }
  }

}
