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
package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.value.CypherRelationship
import org.opencypher.spark_legacy.impl.{StdCypherFrame, StdRuntimeContext}

object TypeFilterRelationship extends FrameCompanion {

  def apply(input: StdCypherFrame[CypherRelationship])(types: Set[String]): StdCypherFrame[CypherRelationship] = {
    TypeFilterRelationship(input)(types)
  }

  private final case class TypeFilterRelationship(input: StdCypherFrame[CypherRelationship])(types: Set[String])
    extends StdCypherFrame[CypherRelationship](input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[CypherRelationship] = {
      val in = input.run
      val out = in.filter(typeFilter(types))
      out
    }
  }

  private final case class typeFilter(types: Set[String]) extends (CypherRelationship => Boolean) {

    override def apply(relationship: CypherRelationship): Boolean = {
      types.forall(t => CypherRelationship.relationshipType(relationship).exists(types))
    }
  }
}
