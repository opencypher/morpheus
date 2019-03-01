/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl.parse.rewriter

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticCheck, SemanticCheckResult, SemanticCheckableExpression}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{nameMatchPatternElements, normalizeMatchPredicates}
import org.neo4j.cypher.internal.v4_0.util._

case class extractSubqueryFromPatternExpression(mkException: (String, InputPosition) => CypherException)
    extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  /**
    * WHERE (a)-[:R]->({foo:true})-->()... AND a.age > 20
    *
    * as well as
    *
    * WHERE EXISTS((a)-[:R]->({foo:true})-->()) ... AND a.age > 20
    *
    * to
    *
    * WHERE EXISTS {
    *   MATCH (a)-[e0]->(v0)-[e1]->(v1)...
    *   WHERE e0:R AND v0.foo = true
    *   RETURN a, true
    * } AND a.age > 20
    */
  private val instance = topDown(Rewriter.lift {
    case f @ FunctionInvocation(_, _, _, IndexedSeq(p : PatternExpression)) if f.function == Exists =>
      rewritePatternExpression(p)

    case p : PatternExpression =>
      rewritePatternExpression(p)
  })

  private def rewritePatternExpression(p: PatternExpression): ExistsPattern = {
    val relationshipsPattern = p.pattern
    val patternPosition: InputPosition = p.position
    val newPattern = Pattern(Seq(EveryPath(relationshipsPattern.element)))(patternPosition)

    val joinVariables = relationshipsPattern.element.treeFold(Seq.empty[LogicalVariable]) {
      case NodePattern(Some(v), _, _, _) =>
        acc =>
          (acc :+ v, None)
      case RelationshipPattern(Some(v), _, _, _, _, _, _) =>
        acc =>
          (acc :+ v, None)
    }

    val returnItems = joinVariables.map(v => AliasedReturnItem(v, v)(v.position))

    val trueVariable = Variable(UnNamedNameGenerator.name(p.position))(p.position)
    val returnItemsWithTrue = returnItems :+ AliasedReturnItem(True()(trueVariable.position), trueVariable)(
      trueVariable.position)

    ExistsPattern(
      Query(
        None,
        SingleQuery(
          Seq(
            Match(optional = false, newPattern, Seq.empty, None)(patternPosition)
              .endoRewrite(nameMatchPatternElements)
              .endoRewrite(normalizeMatchPredicates(getDegreeRewriting = false)),
            Return(ReturnItems(includeExisting = false, returnItemsWithTrue)(patternPosition))(patternPosition)
          )
        )(patternPosition)
      )(patternPosition),
      trueVariable
    )(patternPosition)
  }
}

case class ExistsPattern(query: Query, targetField: Variable)(val position: InputPosition)
    extends Expression
    with SemanticCheckableExpression {
  override def semanticCheck(ctx: Expression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}
