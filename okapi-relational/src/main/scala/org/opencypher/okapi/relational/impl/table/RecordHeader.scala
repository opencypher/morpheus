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
package org.opencypher.okapi.relational.impl.table

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTString, _}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}

object RecordHeader {

  type ExpressionMapping = (Expr, Set[Var])

  implicit class RichExpressionMapping(val mapping: ExpressionMapping) extends AnyVal {

    def expr: Expr = mapping._1

    def key: Expr = expr

    def fields: Set[Var] = mapping._2

    def value: Set[Var] = fields
  }

  type RecordHeader = Map[Expr, Set[Var]]

  val empty: RecordHeader = Map.empty[Expr, Set[Var]]

  def apply(expressions: Expr*): RecordHeader = {
    expressions.foldLeft(RecordHeader.empty)(_.withExpression(_))
  }

  def forNode(node: Var, schema: Schema): RecordHeader = {
    val labels: Set[String] = node.cypherType match {
      case CTNode(l, _) => l
      case other => throw IllegalArgumentException("CTNode", other)
    }
    forNodeWithExplicitLabels(node, schema, labels)
  }

  protected def forNodeWithExplicitLabels(node: Var, schema: Schema, labels: Set[String]): RecordHeader = {
    val labelCombos = if (labels.isEmpty) {
      // all nodes scan
      schema.allLabelCombinations
    } else {
      // label scan
      val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
      schema.combinationsFor(impliedLabels)
    }

    // create a label column for each possible label
    // optimisation enabled: will not add columns for implied or impossible labels
    val labelExprs = labelCombos.flatten.toList.sorted.map { label =>
      HasLabel(node, Label(label))(CTBoolean)
    }

    val properties = schema.keysFor(labelCombos)
    val propertyExprs = properties.toList.sortBy(_._1).map {
      case (k, t) => Property(node, PropertyKey(k))(t)
    }

    RecordHeader(node :: (labelExprs ++ propertyExprs): _*)
  }

  def forRelationship(rel: Var, schema: Schema): RecordHeader = {
    val types: Set[String] = rel.cypherType match {
      case CTRelationship(_types, _) if _types.isEmpty =>
        schema.relationshipTypes
      case CTRelationship(_types, _) =>
        _types
      case other =>
        throw IllegalArgumentException("CTRelationship", other)
    }

    forRelationshipWithExplicitType(rel, schema, types)
  }

  protected def forRelationshipWithExplicitType(rel: Var, schema: Schema, relTypes: Set[String]): RecordHeader = {
    val relKeyHeaderProperties = relTypes.toSeq
      .flatMap(t => schema.relationshipKeys(t).toSeq)
      .groupBy(_._1)
      .mapValues { keys =>
        if (keys.size == relTypes.size && keys.forall(keys.head == _)) {
          keys.head._2
        } else {
          keys.head._2.nullable
        }
      }

    val propertyExprs = relKeyHeaderProperties.toList.sortBy(_._1).map {
      case ((k, t)) => Property(rel, PropertyKey(k))(t)
    }

    val startNode = StartNode(rel)(CTNode)
    val typeString = Type(rel)(CTString)
    val endNode = EndNode(rel)(CTNode)

    RecordHeader(startNode :: rel :: typeString :: endNode :: propertyExprs: _*)
  }

  /**
    * A header for a CypherRecords.
    *
    * The header consists of a number of slots, each of which represents a Cypher expression.
    * The slots that represent variables (which is a kind of expression) are called <i>fields</i>.
    */
  implicit class RichRecordHeader(val header: RecordHeader) extends AnyVal {
    /**
      * The set of record slots stored in this header.
      *
      * @return slots in this header
      */
    def mappings: Set[ExpressionMapping] = header.toSet

    def mappingFor(expr: Expr): ExpressionMapping = expr -> header(expr)

    def expressions: Set[Expr] = header.keySet

    def withMapping(m: ExpressionMapping): RecordHeader = header |+| Map(m)

    def withField(v: Var): RecordHeader = {
      // Ensure a field with the same name and a different type is replaced
      val maybeExistingFieldWithSameName = field(v.name)
      maybeExistingFieldWithSameName match {
        case None =>
          // Add new field
          header |+| Map(v -> Set(v))
        case Some(existing) =>
          if (existing.cypherType == v.cypherType) {
            // Nothing to do, field exists already
            header
          } else {
            // Replace existing field (which could be an alias)
            val (existingKey, oldFields) = header.exprFor(existing)
            val updatedFields = oldFields - existing + v
            header.updated(existingKey, updatedFields)
          }
      }
    }

    def field(name: String): Option[Var] = fields.find(_.name == name)

    def withFields(vs: Var*): RecordHeader = vs.foldLeft(header)(_.withField(_))

    def withMappings(m: ExpressionMapping*): RecordHeader = header |+| m.toMap

    def withExpression(expression: Expr): RecordHeader = {
      expression match {
        case v: Var => withField(v)
        case _ => header |+| Map(expression -> Set.empty)
      }
    }

    def withExpressions(expressions: Expr*): RecordHeader = {
      expressions.foldLeft(header)(_.withExpression(_))
    }

    def selectField(field: Var): RecordHeader = selectFields(Set(field))

    /**
      * Returns the header only for the selected fields.
      */
    def selectFields(fields: Set[Var]): RecordHeader = {
      header.flatMap {
        case (expr, fs) if fs.exists(fields.contains) || expr.owner.exists(fields.contains) =>
          Some(expr -> fs.intersect(fields))
        case _ => None
      }
    }

    /**
      * The set of fields contained in this header.
      *
      * @return the fields in this header.
      */
    def fields: Set[Var] = header.values.flatten.toSet

    def fieldNames: Set[String] = fields.map(_.name)

    def getExprFor(expr: Expr): Option[ExpressionMapping] = {
      if (header.contains(expr)) {
        Some(expr -> header(expr))
      } else {
        expr match {
          case v: Var =>
            // Could be an alias
            header.collectFirst {
              case (maybeAliasedExpr, fields) if fields.contains(v) => maybeAliasedExpr -> fields
            }
          case _ => None
        }
      }
    }

    def exprFor(expr: Expr): ExpressionMapping = {
      getExprFor(expr).getOrElse(
        throw IllegalArgumentException(s"""Expr $expr has no associated expression or alias in header ${header}"""))
    }

    def exprFor(fieldName: String): Option[ExpressionMapping] = {
      fields.collectFirst { case v@Var(name) if name == fieldName => v }.map(exprFor)
    }

    def sourceNodeMapping(rel: Var): ExpressionMapping = exprFor(StartNode(rel)())

    def targetNodeMapping(rel: Var): ExpressionMapping = exprFor(EndNode(rel)())

    def typeMapping(rel: Expr): ExpressionMapping = exprFor(Type(rel)())

    def labels(node: Var): Set[HasLabel] = labelExprs(node).keySet

    def properties(node: Var): Seq[Property] = propertyExprs(node).keys.toSeq

    def ownedExprs(entity: Expr): RecordHeader = {
      val exprMapping = exprFor(entity)
      val entityAliases = exprMapping._2
      header.filter {
        case mapping if mapping.expr.owner.exists(entityAliases.contains) => true
        case _ => false
      }
    }

    def labelExprs(node: Var): Map[HasLabel, Set[Var]] = {
      mappings.collect {
        case (label@HasLabel(n, _), fields) if node == n => label -> fields
      }.toMap
    }

    def propertyExprs(entity: Var): Map[Property, Set[Var]] = {
      mappings.collect {
        case (property@Property(e, _), fields) if entity == e => property -> fields
      }.toMap
    }

    def nodesForType(nodeType: CTNode): Set[Var] = {
      fields.filter { field =>
        field.cypherType match {
          case CTNode(labels, _) =>
            val allPossibleLabels = this.labels(field).map(_.label.name).toSet ++ labels
            nodeType.labels.subsetOf(allPossibleLabels)
          case _ => false
        }
      }
    }

    def relationshipsForType(relType: CTRelationship): Set[Var] = {
      val targetTypes = relType.types

      fields.filter { field =>
        field.cypherType match {
          case t: CTRelationship if targetTypes.isEmpty || t.types.isEmpty => true
          case CTRelationship(types, _) => types.exists(targetTypes.contains)
          case _ => false
        }
      }
    }
  }

}
