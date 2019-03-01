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
package org.opencypher.okapi.testing.propertygraph

import java.util.concurrent.atomic.AtomicLong

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.all._
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations.V2
import org.neo4j.cypher.internal.v4_0.rewriting._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.Never
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, CypherException, InputPosition}
import org.opencypher.okapi.api.value.CypherValue.{CypherEntity, CypherMap, CypherNode, CypherRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper.parseDate
import org.opencypher.okapi.impl.temporal.{Duration, TemporalTypesHelper}

import scala.collection.TraversableOnce
import scala.reflect.ClassTag

object CreateQueryParser {

  val defaultContext: BaseContext = new BaseContext {
    override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

    override def notificationLogger: InternalNotificationLogger = devNullLogger

    override def exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null

    override def monitors: Monitors = new Monitors {
      override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
        new AstRewritingMonitor {
          override def abortedRewriting(obj: AnyRef): Unit = ()

          override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ()
        }
      }.asInstanceOf[T]

      override def addMonitorListener[T](monitor: T, tags: String*): Unit = ()
    }

    override def errorHandler: Seq[SemanticErrorDef] => Unit = _ => ()
  }

  def process(query: String): (Statement, Map[String, Any], SemanticState) = {
    val startState = InitialState(query, None, null, Map.empty)
    val endState = pipeLine.transform(startState, defaultContext)
    val params = endState.extractedParams
    val rewritten = endState.statement
    (rewritten, params, endState.maybeSemantics.get)
  }

  protected val pipeLine: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings(V2) andThen
      PreparatoryRewriting(V2) andThen
      SemanticAnalysis(warn = true).adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, Never, getDegreeRewriting = false) andThen
      SemanticAnalysis(warn = false) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting

}

object CreateGraphFactory extends InMemoryGraphFactory {

  type Result[A] = State[ParsingContext, A]

  def apply(createQuery: String, externalParams: Map[String, Any] = Map.empty): InMemoryTestGraph = {
    val (ast, params, _) = CreateQueryParser.process(createQuery)
    val context = ParsingContext.fromParams(params ++ externalParams)

    ast match {
      case Query(_, SingleQuery(clauses)) => processClauses(clauses).runS(context).value.graph
    }
  }

  def processClauses(clauses: Seq[Clause]): Result[Unit] = {
    clauses match {
      case head :: tail =>
        head match {
          case Create(pattern) =>
            processPattern(pattern, merge = false) >> processClauses(tail)

          case Merge(pattern, _, _) =>
            processPattern(pattern, merge = true) >> processClauses(tail)

          case Unwind(expr, variable) =>
            for {
              values <- processValues(expr)
              _ <- modify[ParsingContext](_.protectScope)
              _ <- Foldable[List].sequence_[Result, Unit](values.map { v =>
                for {
                  _ <- modify[ParsingContext](_.updated(variable.name, v))
                  _ <- processClauses(tail)
                  _ <- modify[ParsingContext](_.restoreScope)
                } yield ()
              })
              _ <- modify[ParsingContext](_.popProtectedScope)
            } yield ()

          case other => throw UnsupportedOperationException(s"Processing clause: ${other.name}")
        }
      case _ => pure[ParsingContext, Unit](())
    }
  }

  def processPattern(pattern: Pattern, merge: Boolean): Result[Unit] = {
    val parts = pattern.patternParts.map {
      case EveryPath(element) => element
      case other => throw UnsupportedOperationException(s"Processing pattern: ${other.getClass.getSimpleName}")
    }

    Foldable[List].sequence_[Result, CypherEntity[Long]](parts.toList.map(pe => processPatternElement(pe, merge)))
  }

  def processPatternElement(patternElement: ASTNode, merge: Boolean): Result[CypherEntity[Long]] = {
    patternElement match {
      case NodePattern(Some(variable), labels, props, _) =>
        for {
          properties <- props match {
            case Some(expr: MapExpression) => extractProperties(expr)
            case Some(other) => throw IllegalArgumentException("a NodePattern with MapExpression", other)
            case None => pure[ParsingContext, CypherMap](CypherMap.empty)
          }
          node <- inspect[ParsingContext, InMemoryTestNode] { context =>
            context.variableMapping.get(variable.name) match {
              case Some(n: InMemoryTestNode) => n
              case Some(other) => throw IllegalArgumentException(s"a Node for variable ${variable.name}", other)
              case None => InMemoryTestNode(context.nextId, labels.map(_.name).toSet, properties)
            }
          }
          _ <- modify[ParsingContext] { context =>
            if (context.variableMapping.get(variable.name).isEmpty) {
              context.updated(variable.name, node, merge)
            } else {
              context
            }
          }
        } yield node

      case RelationshipChain(first, RelationshipPattern(Some(variable), relType, None, props, direction, _, _), third) =>
        for {
          source <- processPatternElement(first, merge)
          sourceId <- pure[ParsingContext, Long](source match {
            case n: CypherNode[Long] => n.id
            case r: CypherRelationship[Long] => r.endId
          })
          target <- processPatternElement(third, merge)
          properties <- props match {
            case Some(expr: MapExpression) => extractProperties(expr)
            case Some(other) => throw IllegalArgumentException("a RelationshipChain with MapExpression", other)
            case None => pure[ParsingContext, CypherMap](CypherMap.empty)
          }
          rel <- inspect[ParsingContext, InMemoryTestRelationship] { context =>
            if (direction == SemanticDirection.OUTGOING)
              InMemoryTestRelationship(context.nextId, sourceId, target.id, relType.head.name, properties)
            else if (direction == SemanticDirection.INCOMING)
              InMemoryTestRelationship(context.nextId, target.id, sourceId, relType.head.name, properties)
            else throw IllegalArgumentException("a directed relationship", direction)
          }

          _ <- modify[ParsingContext](_.updated(variable.name, rel))
        } yield rel
    }
  }

  def extractProperties(expr: MapExpression): Result[CypherMap] = {
    for {
      keys <- pure(expr.items.map(_._1.name))
      values <- expr.items.toList.traverse[Result, Any] {
        case (_, inner) => processExpr(inner)
      }
      res <- pure(CypherMap(keys.zip(values): _*))
    } yield res
  }

  def processExpr(expr: Expression): Result[Any] = {
    for {
      res <- expr match {
        case Parameter(name, _) => inspect[ParsingContext, Any](_.parameter(name))

        case Variable(name) => inspect[ParsingContext, Any](_.variableMapping(name))

        case l: Literal => pure[ParsingContext, Any](l.value)

        case ListLiteral(expressions) => expressions.toList.traverse[Result, Any](processExpr)

        case Modulo(lhs, rhs) =>
          for {
            leftVal <- processExpr(lhs)
            rightVal <- processExpr(rhs)
            res <- pure[ParsingContext, Any](leftVal.asInstanceOf[Long] % rightVal.asInstanceOf[Long])
          } yield res

        case MapExpression(items) =>
          for {
            keys <- pure(items.map { case (k, _) => k.name })
            valueTypes <- items.toList.traverse[Result, Any] { case (_, v) => processExpr(v) }
            res <- pure[ParsingContext, Any](keys.zip(valueTypes).toMap)
          } yield res

        case FunctionInvocation(_, FunctionName("date"), _, Seq(dateString: StringLiteral)) =>
          pure[ParsingContext, Any](parseDate(Right(dateString.value)))

        case FunctionInvocation(_, FunctionName("date"), _, Seq(map: MapExpression)) =>
          for {
            dateMap <- processExpr(map)
            res <- pure[ParsingContext, Any](parseDate(Left(dateMap.asInstanceOf[Map[String, Long]].mapValues(_.toInt))))
          } yield res

        case FunctionInvocation(_, FunctionName("localdatetime"), _, Seq(dateString: StringLiteral)) =>
          pure[ParsingContext, Any](TemporalTypesHelper.parseLocalDateTime(Right(dateString.value)))

        case FunctionInvocation(_, FunctionName("duration"), _, Seq(dateString: StringLiteral)) =>
          pure[ParsingContext, Any](Duration.parse(dateString.value))

        case FunctionInvocation(_, FunctionName("duration"), _, Seq(map: MapExpression)) =>
          for {
            durationMap <- processExpr(map)
            res <- pure[ParsingContext, Any](Duration(durationMap.asInstanceOf[Map[String, Long]]))
          } yield res

        case Property(variable: Variable, propertyKey) =>
          inspect[ParsingContext, Any]({ context =>
            context.variableMapping(variable.name) match {
              case a: CypherEntity[_] => a.properties(propertyKey.name)
              case other =>
                throw UnsupportedOperationException(s"Reading property from a ${other.getClass.getSimpleName}")
            }
          })
        case other =>
          throw UnsupportedOperationException(s"Processing expression of type ${other.getClass.getSimpleName}")
      }
    } yield res
  }

  def processValues(expr: Expression): Result[List[Any]] = {
    expr match {
      case ListLiteral(expressions) => expressions.toList.traverse[Result, Any](processExpr)

      case Variable(name) =>
        inspect[ParsingContext, List[Any]](_.variableMapping(name) match {
          case l: TraversableOnce[Any] => l.toList
          case other => throw IllegalArgumentException(s"a list value for variable $name", other)
        })

      case Parameter(name, _) =>
        inspect[ParsingContext, List[Any]](_.parameter(name) match {
          case l: TraversableOnce[Any] => l.toList
          case other => throw IllegalArgumentException(s"a list value for parameter $name", other)
        })

      case FunctionInvocation(_, FunctionName("range"), _, Seq(lb: IntegerLiteral, ub: IntegerLiteral)) =>
        pure[ParsingContext, List[Any]](List.range[Long](lb.value, ub.value + 1))

      case other => throw UnsupportedOperationException(s"Processing value of type ${other.getClass.getSimpleName}")
    }
  }
}

final case class ParsingContext(
  parameter: Map[String, Any],
  variableMapping: Map[String, Any],
  graph: InMemoryTestGraph,
  protectedScopes: List[Map[String, Any]],
  idGenerator: AtomicLong) {

  def nextId: Long = idGenerator.getAndIncrement()

  def protectScope: ParsingContext = {
    copy(protectedScopes = variableMapping :: protectedScopes)
  }

  def restoreScope: ParsingContext = {
    copy(variableMapping = protectedScopes.head)
  }

  def popProtectedScope: ParsingContext = copy(protectedScopes = protectedScopes.tail)

  def updated(k: String, v: Any, merge: Boolean = false): ParsingContext = v match {
    case n: InMemoryTestNode if !merge || !containsNode(n) =>
      copy(graph = graph.updated(n), variableMapping = variableMapping.updated(k, n))

    case r: InMemoryTestRelationship if !merge || !containsRel(r) =>
      copy(graph = graph.updated(r), variableMapping = variableMapping.updated(k, r))

    case _ =>
      copy(variableMapping = variableMapping.updated(k, v))
  }

  private def containsNode(n: InMemoryTestNode): Boolean =
    graph.nodes.exists(n.equalsSemantically)

  private def containsRel(r: InMemoryTestRelationship): Boolean =
    graph.relationships.exists(r.equalsSemantically)
}

object ParsingContext {
  def fromParams(params: Map[String, Any]): ParsingContext =
    ParsingContext(params, Map.empty, InMemoryTestGraph.empty, List.empty, new AtomicLong())
}
