/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.api.tagging

object TagSupport {

  /**
    * Returns tag remappings that, when applied, guarantee that there are no ID collisions between the
    * given graphs.
    *
    * Optionally fixed retaggings can be supplied. In this case these retaggings are applied and for the remaining
    * graphs the required retaggings are computed on top of the fixed retaggings.
    *
    * @param graphs
    * @param fixedRetaggings
    * @tparam GraphKey
    * @return
    */
  def computeRetaggings[GraphKey](
    graphs: Map[GraphKey, Set[Int]],
    fixedRetaggings: Map[GraphKey, Map[Int, Int]] = Map.empty[GraphKey, Map[Int, Int]]
  ): Map[GraphKey, Map[Int, Int]] = {
    val graphsToRetag = graphs.filterNot { case (qgn, _) => fixedRetaggings.contains(qgn) }
    val usedTags = fixedRetaggings.values.flatMap(_.values).toSet
    val (result, _) = graphsToRetag.foldLeft((fixedRetaggings, usedTags)) {
      case ((graphReplacements, previousTags), (graphId, rightTags)) =>

        val replacements = previousTags.replacementsFor(rightTags)
        val updatedRightTags = rightTags.replaceWith(replacements)

        val updatedPreviousTags = previousTags ++ updatedRightTags
        val updatedGraphReplacements = graphReplacements.updated(graphId, replacements)

        updatedGraphReplacements -> updatedPreviousTags
    }
    result
  }

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
