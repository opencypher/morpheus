/**
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
package org.opencypher.caps.api.types

import scala.language.implicitConversions

object Ternary {
  implicit def apply(v: Boolean): Ternary = if (v) True else False
  implicit def apply(v: Option[Boolean]): Ternary = v.map(Ternary(_)).getOrElse(Maybe)
}

sealed trait Ternary {
  def isTrue: Boolean
  def isFalse: Boolean
  def isDefinite: Boolean
  def isUnknown: Boolean

  def maybeTrue: Boolean
  def maybeFalse: Boolean

  def and(other: Ternary): Ternary
  def or(other: Ternary): Ternary

  def negated: Ternary
}

sealed private[caps] trait DefiniteTernary extends Ternary {
  def isDefinite: Boolean = true
  def isUnknown: Boolean  = false
}

case object True extends DefiniteTernary {
  def isTrue = true
  def isFalse = false

  def maybeTrue = true
  def maybeFalse = false

  def and(other: Ternary): Ternary = other match {
    case True => True
    case False => False
    case Maybe => Maybe
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => True
    case Maybe => True
  }

  def negated = False

  override def toString = "definitely true"
}

case object False extends DefiniteTernary {
  def isTrue = false
  def isFalse = true

  def maybeTrue = false
  def maybeFalse = true

  def and(other: Ternary): Ternary = other match {
    case True => False
    case False => False
    case Maybe => False
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => False
    case Maybe => Maybe
  }

  def negated = True

  override def toString = "definitely false"
}

case object Maybe extends Ternary {
  def isTrue = false
  def isFalse = false
  def isDefinite = false
  def isUnknown = true

  def maybeTrue = true
  def maybeFalse = true

  def and(other: Ternary): Ternary = other match {
    case True => Maybe
    case False => False
    case Maybe => Maybe
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => Maybe
    case Maybe => Maybe
  }

  def negated = Maybe

  override def toString = "maybe"
}
