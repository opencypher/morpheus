package org.opencypher.caps.test.support.testgraph

import java.util.concurrent.atomic.AtomicLong

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.traverse._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.parse.CypherParser

case class ParsingContext(
    parameter: Map[String, Any],
    variableMapping: Map[String, Any],
    graph: PropertyGraph,
    protectedScopes: List[Map[String, Any]],
    idGenerator: AtomicLong) {

  def nextId: Long = idGenerator.getAndIncrement()

  def protect: ParsingContext = {
    copy(protectedScopes = variableMapping :: protectedScopes)
  }

  def clean: ParsingContext = {
    copy(variableMapping = protectedScopes.head)
  }

  def pop: ParsingContext = copy(protectedScopes = protectedScopes.tail)

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

  def apply(createQuery: String): PropertyGraph = {
    val (ast, params, _) = CypherParser.process(createQuery)(CypherParser.defaultContext)
    val context = ParsingContext.fromParams(params)

    ast match {
      case Query(_, SingleQuery(clauses)) => processClauses(clauses).runS(context).value.graph
    }
  }

  def processClauses(clauses: Seq[Clause]): Result[Unit] = {
    clauses match {
      case (head::tail) =>
        head match {
          case Create(pattern) =>
            processPattern(pattern).flatMap(_ => processClauses(tail))

          case Unwind(expr, variable) =>
            for {
              values <- processValues(expr)
              _ <- {
                val a = modify[ParsingContext](_.protect)
                val b = a.flatMap { _ =>
                  Foldable[List].sequence_[Result, Unit](values.map { v =>
                    modify[ParsingContext](_.updated(variable.name, v))
                      .flatMap(_ => processClauses(tail))
                      .flatMap(_ => modify[ParsingContext](_.clean))
                  })
                }
                b.flatMap(_ => modify[ParsingContext](_.pop))
              }
            } yield Unit
        }
      case _ => modify[ParsingContext]{identity}
    }
  }

  def processPattern(pattern: Pattern): Result[Unit] = {
    val parts = pattern.patternParts.map {
      case EveryPath(element) => element
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
          }
          node <- inspect[ParsingContext, Node] { context =>
            context.variableMapping.get(variable.name) match {
              case Some(n: Node) => n
              case None          => Node(context.nextId, labels.map(_.name).toSet, properties)
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
              case None                       => pure[ParsingContext, Map[String, Any]](Map.empty)
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
      }
    } yield res
  }

  def processValues(expr: Expression): Result[List[Any]] = {
//    expr match {
//      ListLiteral
//    }
    pure[ParsingContext, List[Any]](List(1,2,3))
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

