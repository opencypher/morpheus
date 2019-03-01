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
package org.opencypher.okapi.ir.impl.parse

import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.okapi.ir.impl.parse.rewriter.OkapiLateRewriting
import org.opencypher.okapi.ir.impl.typer.toFrontendType
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticErrorDef, SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations.V2
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.Forced

object CypherParser extends CypherParser {
  implicit object defaultContext extends BlankBaseContext {
    override def errorHandler: Seq[SemanticErrorDef] => Unit = errors => {
      // TODO: Remove when frontend supports CLONE clause
      val filteredErrors = errors.filterNot(_.msg.contains("already declared"))
      if (filteredErrors.nonEmpty) {
        throw ParsingException(s"Errors during semantic checking: ${filteredErrors.mkString(", ")}")
      }
    }
  }
}

trait CypherParser {

  def apply(query: String)(implicit context: BaseContext): Statement = process(query)._1

  def process(query: String, drivingTableFields: Set[Var] = Set.empty)
    (implicit context: BaseContext): (Statement, Map[String, Any], SemanticState) = {
    val fieldsWithFrontendTypes = drivingTableFields.map(v => v.name -> toFrontendType(v.cypherType)).toMap
    val startState = InitialState(query, None, null, fieldsWithFrontendTypes)
    val endState = pipeLine.transform(startState, context)
    val params = endState.extractedParams
    val rewritten = endState.statement
    (rewritten, params, endState.maybeSemantics.get)
  }

  protected val pipeLine: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings(V2) andThen
      OkapiPreparatoryRewriting andThen
      SemanticAnalysis(warn = true, SemanticFeature.Cypher10Support, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
        .adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, Forced, getDegreeRewriting = false) andThen
      isolateAggregation andThen
      SemanticAnalysis(warn = false, SemanticFeature.Cypher10Support, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      OkapiLateRewriting
}
