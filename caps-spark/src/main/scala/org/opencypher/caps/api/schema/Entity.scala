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
package org.opencypher.caps.api.schema

import scala.annotation.StaticAnnotation

// TODO: Add docs
// TODO: Move to io package
case class Labels(labels: String*) extends StaticAnnotation

case class RelationshipType(relType: String) extends StaticAnnotation

object Entity {
  private[schema] val sourceIdKey = "id"
}

// TODO: Rename to graph entity
sealed trait Entity extends Product {
  def id: Long
}

/**
  * If a node has no label annotation, then the class name is used as its label.
  * If a `Labels` annotation, for example `@Labels("Person", "Mammal")`, is present,
  * then the labels from that annotation are used instead.
  */
trait Node extends Entity

object Relationship {
  private[schema] val sourceStartNodeKey = "source"

  private[schema] val sourceEndNodeKey = "target"

  private[schema] val nonPropertyAttributes =
    Set(Entity.sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)
}

/**
  * If a relationship has no type annotation, then the class name in all caps is used as its type.
  * If a `Type` annotation, for example `@RelationshipType("FRIEND_OF")` is present,
  * then the type from that annotation is used instead.
  */
trait Relationship extends Entity {
  def source: Long

  def target: Long
}
