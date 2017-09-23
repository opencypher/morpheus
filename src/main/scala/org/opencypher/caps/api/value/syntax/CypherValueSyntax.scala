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
package org.opencypher.caps.api.value.syntax

import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.api.value.{CypherValue, CypherValueCompanion}
import org.opencypher.caps.common.Ternary
import org.opencypher.caps.impl.spark.exception.Raise

import scala.language.implicitConversions

trait CypherValueSyntax {

  implicit def cypherValueOps[V <: CypherValue](value: V): CypherValueOps[V] =
    new CypherValueOps[V](value)
}

final class CypherValueOps[V <: CypherValue](val value: V) extends AnyVal with Serializable {

  import cats.syntax.show._

  def contents(implicit companion: CypherValueCompanion[V]): Option[companion.Contents] =
    companion.contents(value)

  def cypherType(implicit companion: CypherValueCompanion[V]): CypherType =
    companion.cypherType(value)

  def equalTo(other: V)(implicit companion: CypherValueCompanion[V]): Ternary =
    companion.equal(value, other)

  def equivTo(other: V)(implicit companion: CypherValueCompanion[V]): Boolean =
    companion.equiv(value, other)

  def isNull(implicit companion: CypherValueCompanion[V]): Boolean =
    companion.isNull(value)

  def comparesNulls(implicit companion: CypherValueCompanion[V]): Boolean =
    companion.comparesNulls(value)

  def <(other: V)(implicit companion: CypherValueCompanion[V]): Ternary =
    companion
      .compare(value, other)
      .map(_ < 0)
      .getOrElse(Raise.incomparableArguments(value.show, other.show))

  def <=(other: V)(implicit companion: CypherValueCompanion[V]): Ternary =
    companion
      .compare(value, other)
      .map(_ <= 0)
      .getOrElse(Raise.incomparableArguments(value.show, other.show))

  def >(other: V)(implicit companion: CypherValueCompanion[V]): Ternary =
    companion
      .compare(value, other)
      .map(_ > 0)
      .getOrElse(Raise.incomparableArguments(value.show, other.show))

  def >=(other: V)(implicit companion: CypherValueCompanion[V]): Ternary = companion
    .compare(value, other)
    .map(_ >= 0)
    .getOrElse(Raise.incomparableArguments(value.show, other.show))
}
