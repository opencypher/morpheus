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

import org.opencypher.okapi.impl.exception.IllegalStateException

object Tags {

  // Long representation using 64 bits containing graph tag and entity identifier
  val totalBits: Int = 64

  // Number of Bits used to store entity identifier
  val idBits: Int = 54

  val tagBits = totalBits - idBits

  // 1023 with 10 tag bits
  val maxTag: Int = (1 << tagBits) - 1

  // All possible tags, 1024 entries with 10 tag bits.
  val allTags: Set[Int] = (0 to maxTag).toSet

  // Mask to extract graph tag
  val tagMask: Long = -1L << idBits

  val invertedTagMask: Long = ~tagMask

  /**
    * Returns a free tag given a set of used tags.
    */
  def pickFreeTag(usedTags: Set[Int]): Int = {
    if (usedTags.isEmpty) {
      0
    } else {
      val maxUsed = usedTags.max
      if (maxUsed < maxTag) {
        maxUsed + 1
      } else {
        val availableTags = allTags -- usedTags
        if (availableTags.nonEmpty) {
          availableTags.min
        } else {
          throw IllegalStateException("Could not complete this operation, ran out of tag space.")
        }
      }
    }
  }

}
