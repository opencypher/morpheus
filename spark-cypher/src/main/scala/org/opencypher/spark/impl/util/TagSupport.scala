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
package org.opencypher.spark.impl.util

import org.opencypher.okapi.api.graph.QualifiedGraphName

//
//import org.opencypher.okapi.api.schema.{RelTypePropertyMap, Schema}
//
//trait TagSupport {
//
//  self: Schema with TagSupport =>
//
//  def tags: Set[Int] = Set(0)
//
//  def withTag(tag: Int): Schema with TagSupport = withTags(tag)
//
//  def withTags(tags: Int*): Schema with TagSupport = withTags(tags.toSet)
//
//  def withTags(tags: Set[Int]): Schema with TagSupport
//
//  def replaceTags(replacements: Map[Int, Int]): Schema with TagSupport
//
//  def union(other: Schema with TagSupport): Schema with TagSupport
//}
//
object TagSupport {

  def computeRetaggings(graphs: Map[QualifiedGraphName, Set[Int]]): Map[QualifiedGraphName, Map[Int, Int]] = {
    val (result, _) = graphs.foldLeft((Map.empty[QualifiedGraphName, Map[Int, Int]], Set.empty[Int])) {
      case ((graphReplacements, previousTags), (graphId, rightTags)) =>

        val replacements = previousTags.replacementsFor(rightTags)
        val updatedRightTags = rightTags.replaceWith(replacements)

        val updatedPreviousTags = previousTags ++ updatedRightTags
        val updatedGraphReplacements = graphReplacements.updated(graphId, replacements)

        updatedGraphReplacements -> updatedPreviousTags
    }
    result
  }


//  implicit class TaggedSchema(s: Schema) {
//
//    def toTagged: Schema with TagSupport = {
//      s match {
//        case swt: Schema with TagSupport => swt
//        case other => SchemaImpl(other.labelPropertyMap, other.relTypePropertyMap: RelTypePropertyMap, Set(0))
//      }
//    }
//
//    def withTag(tag: Int): Schema with TagSupport = withTags(tag)
//
//    def withTags(tags: Int*): Schema with TagSupport = withTags(tags.toSet)
//
//    def withTags(tags: Set[Int]): Schema with TagSupport = s.toTagged.withTags(tags)
//
//  }

  implicit class TagSet(val lhsTags: Set[Int]) extends AnyVal {

    /**
      * Computes replacement tags for the rhs tags, such that there are no collisions between the lhsTags.
      *
      * The values of the output map will not intersect with the lhsTags
      */
    def replacementsFor(rhsTags: Set[Int]): Map[Int, Int] = {
      if (lhsTags.isEmpty && rhsTags.isEmpty) {
        Map.empty
      } else if (lhsTags.isEmpty) {
        rhsTags.zip(rhsTags).toMap
      } else if (rhsTags.isEmpty) {
        lhsTags.zip(lhsTags).toMap
      } else {
        val maxUsedTag = Math.max(lhsTags.max, rhsTags.max)
        val nextTag = maxUsedTag + 1
        val conflicts = lhsTags intersect rhsTags

        val conflictMap = conflicts.zip(nextTag until nextTag + conflicts.size).toMap

        val nonConflicts = rhsTags -- conflicts
        val identityMap = nonConflicts.zip(nonConflicts).toMap

        conflictMap ++ identityMap
      }
    }

    def replaceWith(replacements: Map[Int, Int]): Set[Int] =
      lhsTags.map(t => replacements.getOrElse(t, identity(t)))

  }

}
