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
package org.opencypher.caps.ir.impl.parse.rewriter

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{nameMatchPatternElements, normalizeMatchPredicates}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckResult, SemanticCheckableExpression}
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.functions.Exists

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
      case NodePattern(Some(v), _, _) =>
        (acc) =>
          (acc :+ v, None)
      case RelationshipPattern(Some(v), _, _, _, _, _) =>
        (acc) =>
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
            Return(ReturnItems(includeExisting = false, returnItemsWithTrue)(patternPosition), None)(patternPosition)
          )
        )(patternPosition)
      )(patternPosition)
    )(patternPosition)
  }
}

case class ExistsPattern(query: Query)(val position: InputPosition)
    extends Expression
    with SemanticCheckableExpression {
  override def semanticCheck(ctx: Expression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}
