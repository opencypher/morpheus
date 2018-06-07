/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.impl

import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSFunctions.{array_contains, get_node_labels, get_property_keys, get_rel_type}
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.physical.CAPSRuntimeContext

object SparkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.expressionsFor(expr).isEmpty) throw IllegalStateException(s"Expression $expr not in header ${header.pretty}")
    }

    /**
      * This is possible without violating Cypher semantics because
      *   - Spark SQL returns null when comparing across types (from initial investigation)
      *   - We never have multiple types per column in CAPS (yet)
      */
    def compare(comparator: Column => (Column => Column), lhs: Expr, rhs: Expr)
      (implicit header: RecordHeader, df: DataFrame, context: CAPSRuntimeContext): Column = {
      comparator(lhs.asSparkSQLExpr)(rhs.asSparkSQLExpr)
    }

    def lt(c: Column): Column => Column = c < _

    def lteq(c: Column): Column => Column = c <= _

    def gt(c: Column): Column => Column = c > _

    def gteq(c: Column): Column => Column = c >= _

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header  the header of the CAPSRecords in which the expression should be evaluated.
      * @param df      the dataframe containing the data over which the expression should be evaluated.
      * @param context context with helper functions, such as column names.
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, context: CAPSRuntimeContext): Column = {

      expr match {

        // context based lookups
        case p@Param(name) if p.cypherType.isInstanceOf[CTList] =>
          context.parameters(name) match {
            case CypherList(l) => functions.array(l.unwrap.map(functions.lit): _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }
        case Param(name) =>
          functions.lit(context.parameters(name).unwrap)

        // direct column lookup
        case _: Var | _: Param | _: Property | _: HasLabel | _: HasType | _: StartNode | _: EndNode =>
          verify

          val colName = header.column(expr)
          if (df.columns.contains(colName)) {
            df.col(colName)
          } else {
            functions.lit(null)
          }

        // Literals
        case ListLit(exprs) =>
          functions.array(exprs.map(_.asSparkSQLExpr): _*)

        case l: Lit[_] => functions.lit(l.v)

        // predicates
        case Equals(e1, e2) => e1.asSparkSQLExpr === e2.asSparkSQLExpr
        case Not(e) => !e.asSparkSQLExpr
        case IsNull(e) => e.asSparkSQLExpr.isNull
        case IsNotNull(e) => e.asSparkSQLExpr.isNotNull
        case Size(e) =>
          val col = e.asSparkSQLExpr
          e.cypherType match {
            case CTString => functions.length(col).cast(LongType)
            case _: CTList => functions.size(col).cast(LongType)
            case other => throw NotImplementedException(s"size() on values of type $other")
          }

        case Ands(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(functions.lit(true))(_ && _)

        case Ors(exprs) =>
          exprs.map(_.asSparkSQLExpr).foldLeft(functions.lit(false))(_ || _)

        case In(lhs, rhs) =>
          if (rhs.cypherType == CTNull || lhs.cypherType == CTNull) {
            functions.lit(null).cast(BooleanType)
          } else {
            val element = lhs.asSparkSQLExpr
            val array = rhs.asSparkSQLExpr
            array_contains(array, element)
          }

        case LessThan(lhs, rhs) => compare(lt, lhs, rhs)
        case LessThanOrEqual(lhs, rhs) => compare(lteq, lhs, rhs)
        case GreaterThanOrEqual(lhs, rhs) => compare(gteq, lhs, rhs)
        case GreaterThan(lhs, rhs) => compare(gt, lhs, rhs)

        // Arithmetics
        case Add(lhs, rhs) => lhs.asSparkSQLExpr + rhs.asSparkSQLExpr
        case Subtract(lhs, rhs) => lhs.asSparkSQLExpr - rhs.asSparkSQLExpr
        case Multiply(lhs, rhs) => lhs.asSparkSQLExpr * rhs.asSparkSQLExpr
        case div@Divide(lhs, rhs) => (lhs.asSparkSQLExpr / rhs.asSparkSQLExpr).cast(div.cypherType.getSparkType)

        // Functions
        case Exists(e) => e.asSparkSQLExpr.isNotNull
        case Id(e) => e.asSparkSQLExpr
        case Labels(e) =>
          val node = e.owner.get
          val labelExprs = header.labelsFor(node)
          val (labelNames, labelColumns) = labelExprs.toSeq.map(e => e.label.name -> e.asSparkSQLExpr).unzip
          val booleanLabelFlagColumn = functions.array(labelColumns: _*)
          get_node_labels(labelNames)(booleanLabelFlagColumn)

        case Keys(e) =>
          val node = e.owner.get
          val propertyExprs = header.propertiesFor(node)
          val (propertyKeys, propertyColumns) = propertyExprs.toSeq.map(e => e.key.name -> e.asSparkSQLExpr).unzip
          val valuesColumn = functions.array(propertyColumns: _*)
          get_property_keys(propertyKeys)(valuesColumn)

        case Type(inner) =>
          inner match {
            case v: Var =>
              val typeExprs = header.typesFor(v)
              val (relTypeNames, relTypeColumn) = typeExprs.toSeq.map(e => e.relType.name -> e.asSparkSQLExpr).unzip
              val booleanLabelFlagColumn = functions.array(relTypeColumn: _*)
              get_rel_type(relTypeNames)(booleanLabelFlagColumn)
            case _ =>
              throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
          }

        case StartNodeFunction(e) =>
          val rel = e.owner.get
          header.startNodeFor(rel).asSparkSQLExpr

        case EndNodeFunction(e) =>
          val rel = e.owner.get
          header.endNodeFor(rel).asSparkSQLExpr

        case ToFloat(e) => e.asSparkSQLExpr.cast(DoubleType)

        case Explode(list) => list.cypherType match {
          case CTList(_) | CTListOrNull(_) => functions.explode(list.asSparkSQLExpr)
          case other => throw IllegalArgumentException("CTList", other)
        }

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          functions.coalesce(columns: _*)

        case c: CaseExpr =>
          val alternatives = c.alternatives.map {
            case (predicate, action) => functions.when(predicate.asSparkSQLExpr, action.asSparkSQLExpr)
          }

          val alternativesWithDefault = c.default match {
            case Some(inner) => alternatives :+ inner.asSparkSQLExpr
            case None => alternatives
          }

          val reversedColumns = alternativesWithDefault.reverse

          val caseColumn = reversedColumns.tail.foldLeft(reversedColumns.head) {
            case (tmpCol, whenCol) => whenCol.otherwise(tmpCol)
          }
          caseColumn

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }
  }

}
