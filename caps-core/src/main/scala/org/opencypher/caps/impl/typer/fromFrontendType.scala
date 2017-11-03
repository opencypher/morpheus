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
package org.opencypher.caps.impl.typer

import org.neo4j.cypher.internal.frontend.v3_3.{symbols => frontend}
import org.opencypher.caps.api.types._

// TODO: Should go to option I think
case object fromFrontendType extends (frontend.CypherType => CypherType) {
  override def apply(in: frontend.CypherType): CypherType = in match {
    case frontend.CTAny => CTAny
    case frontend.CTNumber => CTNumber
    case frontend.CTInteger => CTInteger
    case frontend.CTFloat => CTFloat
    case frontend.CTBoolean => CTBoolean
    case frontend.CTString => CTString
    case frontend.CTMap => CTMap
    case frontend.CTNode => CTNode
    case frontend.CTRelationship => CTRelationship
    case frontend.ListType(inner) => CTList(fromFrontendType(inner))
    case x => throw new UnsupportedOperationException(s"No support for type $x")
  }
}
