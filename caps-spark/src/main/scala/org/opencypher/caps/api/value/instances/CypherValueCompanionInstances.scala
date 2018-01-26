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
package org.opencypher.caps.api.value.instances

import org.opencypher.caps.api.value._

trait CypherValueCompanionInstances {
  implicit def cypherValueCompanion: CypherValue.type = CypherValue
  implicit def cypherBooleanCompanion: CypherBoolean.type = CypherBoolean
  implicit def cypherStringCompanion: CypherString.type = CypherString
  implicit def cypherFloatCompanion: CypherFloat.type = CypherFloat
  implicit def cypherIntegerCompanion: CypherInteger.type = CypherInteger
  implicit def cypherNumberCompanion: CypherNumber.type = CypherNumber
  implicit def cypherListCompanion: CypherList.type = CypherList
  implicit def cypherMapCompanion: CypherMap.type = CypherMap
  implicit def cypherNodeCompanion: CypherNode.type = CypherNode
  implicit def cypherRelationshipCompanion: CypherRelationship.type = CypherRelationship
  implicit def cypherPathCompanion: CypherPath.type = CypherPath
}
