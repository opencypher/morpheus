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
package org.opencypher.caps.api.ir.pattern

import org.opencypher.caps.api.ir.Field

import scala.language.implicitConversions

sealed trait Endpoints extends Traversable[Field] {
  def contains(f: Field): Boolean
}

case object Endpoints {

  def apply(source: Field, target: Field): Endpoints =
    if (source == target) OneSingleEndpoint(source) else TwoDifferentEndpoints(source, target)

  implicit def one(field: Field): IdenticalEndpoints =
    OneSingleEndpoint(field)

  implicit def two(fields: (Field, Field)): Endpoints = {
    val (source, target) = fields
    apply(source, target)
  }

  private case class OneSingleEndpoint(field: Field) extends IdenticalEndpoints {
    override def contains(f: Field) = field == f
  }
  private case class TwoDifferentEndpoints(source: Field, target: Field) extends DifferentEndpoints {
    override def flip = copy(target, source)

    override def contains(f: Field) = f == source || f == target
  }
}

sealed trait IdenticalEndpoints extends Endpoints {
  def field: Field

  final override def foreach[U](f: (Field) => U): Unit = f(field)
}

sealed trait DifferentEndpoints extends Endpoints {
  def source: Field
  def target: Field

  def flip: DifferentEndpoints

  override def foreach[U](f: (Field) => U): Unit = { f(source); f(target) }
}
