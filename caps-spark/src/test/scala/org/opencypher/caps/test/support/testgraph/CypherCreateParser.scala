package org.opencypher.caps.test.support.testgraph

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.opencypher.caps.impl.parse.CypherParser


case class ParsingContext(
    parameter: Map[String, Any],
    variableMapping: Map[String, Any]) {

  private val idGenerator = new AtomicLong

  def nextId: Long = idGenerator.incrementAndGet()

  def updated(k: String, v: Any): ParsingContext = copy(variableMapping = variableMapping.updated(k,v))
}

object CypherCreateParser {

  def apply(createQuery: String): PropertyGraph = {
    val (ast, params, _) = CypherParser.process(createQuery)(CypherParser.defaultContext)
    val state = PropertyGraph.empty
    val context = ParsingContext(params, Map.empty)

    ast match {
      case Query(_, SingleQuery(clauses)) => processClauses(clauses)(state, context)._1
    }
  }

  def processClauses(clause: Seq[Clause])(state: PropertyGraph, context: ParsingContext): (PropertyGraph, ParsingContext) = {
    clause match  {
      case (head :: tail) =>
        head match {
          case Create(pattern) =>
            val newState = processPattern(pattern)(state, context)
            processClauses(tail)(newState, context)
          case Unwind(expr, variable) =>
            val (newState, newContext) = processUnwind(expr, variable)(state, context)
            processClauses(tail)(newState, newContext)
        }
      case List() => state -> context
    }
  }

  def processPattern(pattern: Pattern)(state: PropertyGraph, context: ParsingContext): PropertyGraph = {
    val parts = pattern.patternParts.map {
      case EveryPath(element) => element
    }

    parts.foldLeft(state){
      case (acc, pe: PatternElement) => processPatternElement(pe)(acc, context)
    }
  }

  def processPatternElement(patternElement: ASTNode)(implicit state: PropertyGraph, context: ParsingContext): PropertyGraph = {
    patternElement match {
      case NodePattern(Some(variable), labels, Some(properties: MapExpression)) =>
        if (state.getElementByVariable(variable.name).isEmpty) {
          val newNode = Node(variable.name, context.nextId, labels.map(_.name).toSet, extractProperties(properties))
          state.updated(newNode)
        } else {
          state
        }

      case RelationshipChain(first, RelationshipPattern(Some(variable), relType, None, props, _,_), target) =>
        val graphWithNodeAndTarget = Seq(first, target).foldLeft(state) {
          case (acc, element) => processPatternElement(element)(acc, context)
        }

        def variableToId(v: String, g: PropertyGraph): Long = {
          g.getElementByVariable(v).get.id
        }

        val sourceId = first match {
          case NodePattern(Some(v), _, _) => variableToId(v.name, graphWithNodeAndTarget)
          case RelationshipChain(_, _, NodePattern(Some(v), _, _))  => variableToId(v.name, graphWithNodeAndTarget)
        }

        val targetId = variableToId(target.variable.get.name, graphWithNodeAndTarget)

       val newRel = props match  {
         case Some(properties: MapExpression) =>
           Relationship(variable.name, context.nextId, sourceId, targetId, relType.head.name, extractProperties(properties))
         case _ =>
           Relationship(variable.name, context.nextId, sourceId, targetId, relType.head.name, Map.empty)
       }

       graphWithNodeAndTarget.updated(newRel)
    }
  }

  def processUnwind(expr: Expression, variable: Variable)(implicit state: PropertyGraph, context: ParsingContext): (PropertyGraph, ParsingContext) = ???

  def extractProperties(expr: MapExpression)(implicit state: PropertyGraph, context: ParsingContext): Map[String, Any] =
    expr.items.map {
      case (PropertyKeyName(name), inner: Expression) => name -> processExpr(inner)
    }.toMap


  def processExpr(expr: Expression)(implicit state: PropertyGraph, context: ParsingContext): Any = {
    expr match {
      case Parameter(name, _) => context.parameter(name)

      case Variable(name) => context.variableMapping(name)
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

  def getElementByVariable(variable: String): Option[GraphElement] = {
    (nodes ++ relationships).collectFirst {
      case e: GraphElement if e.variable == variable => e
    }
  }
}

trait GraphElement {
  def id: Long
  def variable: String
  def properties: Map[String, Any]
}

case class Node(variable: String, id: Long, labels: Set[String], properties: Map[String, Any]) extends GraphElement

case class Relationship(
    variable: String,
    id: Long,
    startId: Long,
    endId: Long,
    relType: String,
    properties: Map[String, Any]
) extends GraphElement

case class PropertyGraph(nodes: Seq[Node], relationships: Seq[Relationship]) extends  Graph {
  def updated(node: Node): PropertyGraph = copy(nodes = node +: nodes)

  def updated(rel: Relationship): PropertyGraph = copy(relationships = rel +: relationships)
}

object PropertyGraph {
  def empty: PropertyGraph = PropertyGraph(Seq.empty, Seq.empty)
}