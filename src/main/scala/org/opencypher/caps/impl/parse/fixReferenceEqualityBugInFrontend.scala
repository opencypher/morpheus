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
package org.opencypher.caps.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.StatementRewriter
import org.neo4j.cypher.internal.frontend.v3_3.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}

// TODO: Remove this once we have merged https://github.com/neo4j/neo4j/pull/10083
case object fixReferenceEqualityBugInFrontend extends StatementRewriter {

  private val rewriter = Rewriter.lift {
    case graphItem@GraphAs(v1, Some(v2), _) if v1 == v2 =>
      graphItem.copy(as = Some(Variable(v2.name)(v2.position)))(graphItem.position)
  }

  override def instance(context: BaseContext) = bottomUp(rewriter, _.isInstanceOf[Expression])

  override def description = "fixes a bug in the fronted, which caused the same variable instance to be used in multiple places"

  override def postConditions: Set[Condition] = Set.empty
}
