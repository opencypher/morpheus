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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.impl.schema

import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}

trait TagSupport {

  self: Schema with TagSupport =>

  def tags: Set[Int] = Set(0)

  def withTag(tag: Int): Schema with TagSupport = withTags(tag)

  def withTags(tags: Int*): Schema with TagSupport = withTags(tags.toSet)

  def withTags(tags: Set[Int]): Schema with TagSupport

  def replaceTags(replacements: Map[Int, Int]): Schema with TagSupport

  def union(other: Schema with TagSupport): Schema with TagSupport
}

object TagSupport {

  implicit class TaggedSchema(s: Schema) {

    def withTag(tag: Int): Schema with TagSupport = withTags(tag)

    def withTags(tags: Int*): Schema with TagSupport = withTags(tags.toSet)

    def withTags(tags: Set[Int]): Schema with TagSupport = {
       s match {
         case swt: Schema with TagSupport => swt.withTags(tags)
         case other => SchemaImpl(other.labelPropertyMap, other.relTypePropertyMap: RelTypePropertyMap, tags)
       }
    }
  }

  implicit class TagSet(val tags: Set[Int]) extends AnyVal {

    def replacementsFor(rightTags: Set[Int]): Map[Int, Int] = {
      if (tags.isEmpty) {
        Map.empty
      } else {
        val maxUsedTag = Math.max(tags.max, rightTags.max)
        val nextTag = maxUsedTag + 1
        val conflicts = tags intersect rightTags

        conflicts.zip(nextTag until nextTag + conflicts.size).toMap
      }
    }

    def replaceWith(replacements: Map[Int, Int]): Set[Int] =
      tags.map(t => replacements.getOrElse(t, identity(t)))

  }
}
