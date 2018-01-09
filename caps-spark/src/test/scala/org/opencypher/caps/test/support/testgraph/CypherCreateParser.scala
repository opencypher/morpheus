package org.opencypher.caps.test.support.testgraph

import java.util.concurrent.atomic.AtomicLong

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.all._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.parse.CypherParser

final case class ParsingContext(
    parameter: Map[String, Any],
    variableMapping: Map[String, Any],
    graph: PropertyGraph,
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

  def updated(k: String, v: Any): ParsingContext = v match {
    case n: Node =>
      copy(graph = graph.updated(n), variableMapping = variableMapping.updated(k, n))

    case r: Relationship =>
      copy(graph = graph.updated(r), variableMapping = variableMapping.updated(k, r))

    case _ =>
      copy(variableMapping = variableMapping.updated(k, v))
  }
}

object ParsingContext {
  def fromParams(params: Map[String, Any]): ParsingContext =
    ParsingContext(params, Map.empty, PropertyGraph.empty, List.empty, new AtomicLong())
}

object CypherCreateParser {

  type Result[A] = State[ParsingContext, A]

  def apply(createQuery: String, externalParams: Map[String, Any] = Map.empty): PropertyGraph = {
    val (ast, params, _) = CypherParser.process(createQuery)(CypherParser.defaultContext)
    val context = ParsingContext.fromParams(params ++ externalParams)

    ast match {
      case Query(_, SingleQuery(clauses)) => processClauses(clauses).runS(context).value.graph
    }
  }

  def processClauses(clauses: Seq[Clause]): Result[Unit] = {
    clauses match {
      case (head::tail) =>
        head match {
          case Create(pattern) =>
            processPattern(pattern) >> processClauses(tail)

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

          case other => Raise.unsupportedOperation(other.name)
        }
      case _ => pure[ParsingContext,Unit](())
    }
  }

  def processPattern(pattern: Pattern): Result[Unit] = {
    val parts = pattern.patternParts.map {
      case EveryPath(element) => element
      case other => Raise.unsupportedOperation(other.getClass.getSimpleName)
    }

    Foldable[List].sequence_[Result, GraphElement](parts.toList.map(pe => processPatternElement(pe)))
  }

  def processPatternElement(patternElement: ASTNode): Result[GraphElement] = {
    patternElement match {
      case NodePattern(Some(variable), labels, props) =>
        for {
          properties <- props match {
            case Some(expr: MapExpression) => extractProperties(expr)
            case None                      => pure[ParsingContext, Map[String, Any]](Map.empty)
            case _                         => Raise.impossible()
          }
          node <- inspect[ParsingContext, Node] { context =>
            context.variableMapping.get(variable.name) match {
              case Some(n: Node) => n
              case None          => Node(context.nextId, labels.map(_.name).toSet, properties)
              case _             => Raise.impossible()
            }
          }
          _ <- modify[ParsingContext] { context =>
            if (context.variableMapping.get(variable.name).isEmpty) {
              context.updated(variable.name, node)
            } else {
              context
            }
          }
        } yield node

      case RelationshipChain(first, RelationshipPattern(Some(variable), relType, None, props, _, _), third) =>
        for {
          source <- processPatternElement(first)
          sourceId <- pure[ParsingContext, Long](source match {
            case Node(id, _, _) => id
            case r: Relationship => r.endId
          })
          target <- processPatternElement(third)
          properties <- props match {
              case Some(expr: MapExpression) => extractProperties(expr)
              case None                      => pure[ParsingContext, Map[String, Any]](Map.empty)
              case _                         => Raise.impossible()
          }
          rel <- inspect[ParsingContext, Relationship] { context =>
            Relationship(context.nextId, sourceId, target.id, relType.head.name, properties)
          }

          _ <- modify[ParsingContext](_.updated(variable.name, rel))
        } yield rel
    }
  }

  def extractProperties(expr: MapExpression): Result[Map[String, Any]] = {
    for  {
      keys <- pure(expr.items.map(_._1.name))
      values <- expr.items.toList.traverse[Result, Any] {
          case (_, inner: Expression) => processExpr(inner)
          case _ => Raise.impossible()
      }
      res <- pure(keys.zip(values).toMap)
    } yield res
  }


  def processExpr(expr: Expression): Result[Any] = {
    for {
      res <- expr match {
        case Parameter(name, _) => inspect[ParsingContext, Any](_.parameter(name))
        case Variable(name)     => inspect[ParsingContext, Any](_.variableMapping(name))
        case l:Literal          => pure[ParsingContext, Any](l.value)
        case ListLiteral(expressions) => expressions.toList.traverse[Result, Any](processExpr)
        case other => Raise.unsupportedOperation(other.getClass.getSimpleName)
      }
    } yield res
  }

  def processValues(expr: Expression): Result[List[Any]] = {
    expr match {
      case ListLiteral(expressions) => expressions.toList.traverse[Result, Any](processExpr)
      case Variable(name)           => inspect[ParsingContext, List[Any]](_.variableMapping(name) match {
        case l : List[Any] => l
        case _             => Raise.impossible()
      })
      case Parameter(name, _)     => inspect[ParsingContext, List[Any]](_.parameter(name) match {
        case l : List[Any] => l
        case _             => Raise.impossible()
      })
    }
  }
}

trait Graph {
  def nodes: Seq[Node]
  def relationships: Seq[Relationship]

  def getNodeById(id: Long): Option[Node] = {
    nodes.collectFirst {
      case n : Node if n.id == id => n
    }
  }

  def getRelationshipById(id: Long): Option[Relationship] = {
    relationships.collectFirst {
      case r : Relationship if r.id == id => r
    }
  }
}

trait GraphElement {
  def id: Long
  def properties: Map[String, Any]
}

case class PropertyGraph(nodes: Seq[Node], relationships: Seq[Relationship]) extends Graph {
  def updated(node: Node): PropertyGraph = copy(nodes = node +: nodes)

  def updated(rel: Relationship): PropertyGraph = copy(relationships = rel +: relationships)
}

object PropertyGraph {
  def empty: PropertyGraph = PropertyGraph(Seq.empty, Seq.empty)
}

case class Node(id: Long, labels: Set[String], properties: Map[String, Any]) extends GraphElement

case class Relationship(
  id: Long,
  startId: Long,
  endId: Long,
  relType: String,
  properties: Map[String, Any]
) extends GraphElement

