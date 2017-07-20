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
import org.opencypher.spark.api.types.CTRelationshipOrNull
import org.opencypher.spark.api.value.CypherRelationship
import org.opencypher.spark_legacy.impl.{PlanningContext, RelationshipFrame, StdCypherFrame, StdFrameSignature}

object OptionalAllRelationships {

  def apply(relationship: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherRelationship] = {
    val (_, sig) = StdFrameSignature.empty.addField(relationship -> CTRelationshipOrNull)

    AllRelationshipsOrNull(context.relationships)(sig)
  }

  private final case class AllRelationshipsOrNull(input: Dataset[CypherRelationship])(sig: StdFrameSignature)
    extends RelationshipFrame(sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherRelationship] = {
      if (input.rdd.isEmpty())
        context.session.createDataset[CypherRelationship](Seq(null))(context.cypherRelationshipEncoder)
      else input
    }
  }
}
