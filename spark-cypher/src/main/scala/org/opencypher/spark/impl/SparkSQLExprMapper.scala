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
package org.opencypher.spark.impl

import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.functions.{array_contains => _, translate => _, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.impl.temporal.TemporalTypesHelper._
import org.opencypher.okapi.impl.temporal.{Duration => DurationValue}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSFunctions._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.expressions.AddPrefix._
import org.opencypher.spark.impl.expressions.EncodeLong._
import org.opencypher.spark.impl.temporal.SparkTemporalHelpers._
import org.opencypher.spark.impl.temporal.TemporalUdfs

final case class SparkSQLMappingException(msg: String) extends InternalException(msg)

object SparkSQLExprMapper {

  private val NULL_LIT: Column = lit(null)
  private val TRUE_LIT: Column = lit(true)
  private val FALSE_LIT: Column = lit(false)
  private val ONE_LIT: Column = lit(1)
  private val E_LIT: Column = lit(Math.E)
  private val PI_LIT: Column = lit(Math.PI)

  implicit class RichExpression(expr: Expr) {

    /**
      * Attempts to create a Spark SQL expression from the CAPS expression.
      *
      * @param header     the header of the CAPSRecords in which the expression should be evaluated.
      * @param df         the dataframe containing the data over which the expression should be evaluated.
      * @param parameters query parameters
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {

      convert(expr) { convertedChildren =>
        def c1: Column = convertedChildren.head
        def c2: Column = convertedChildren(1)
        def c3: Column = convertedChildren(2)

        expr match {
          case _: ListLit => array(convertedChildren: _*)
          case l: Lit[_] => lit(l.v)
          case _: Var | _: HasLabel | _: HasType | _: StartNode | _: EndNode => columnFor(expr)
          case _: AliasExpr => c1
          case Param(name) => toSparkLiteral(parameters(name).unwrap)

          // Predicates
          case _: Equals => c1 === c2
          case _: Not => !c1
          case Size(e) => {
            e.cypherType match {
              case CTString => length(c1)
              case _ => size(c1) // it's a list
            }
          }.cast(LongType)
          case _: Ands => convertedChildren.foldLeft(TRUE_LIT)(_ && _)
          case _: Ors => convertedChildren.foldLeft(FALSE_LIT)(_ || _)
          case _: IsNull => c1.isNull
          case _: IsNotNull => c1.isNotNull
          case _: Exists => c1.isNotNull
          case _: LessThan => c1 < c2
          case _: LessThanOrEqual => c1 <= c2
          case _: GreaterThanOrEqual => c1 >= c2
          case _: GreaterThan => c1 > c2

          case _: StartsWith => c1.startsWith(c2)
          case _: EndsWith => c1.endsWith(c2)
          case _: Contains => c1.contains(c2)
          case _: RegexMatch => regex_match(c1, c2)

          // Other
          case _: Explode => explode(c1)
          case Property(e, PropertyKey(key)) =>
            // Convert property lookups into separate specific lookups instead of overloading
            e.cypherType.material match {
              case CTMap(inner) => if (inner.keySet.contains(key)) c1.getField(key) else NULL_LIT
              case CTDate => temporalAccessor[java.sql.Date](c1, key)
              case CTLocalDateTime => temporalAccessor[java.sql.Timestamp](c1, key)
              case CTDuration => TemporalUdfs.durationAccessor(key.toLowerCase).apply(c1)
              case _ =>
                // TODO: Investigate this case
                if (!header.contains(expr)) {
                  NULL_LIT
                } else {
                  columnFor(expr)
                }
            }
          case LocalDateTime(dateExpr) =>
            // TODO: Move code outside of expr mapper
            dateExpr match {
              case Some(e) =>
                val localDateTimeValue = resolveTemporalArgument(e)
                  .map(parseLocalDateTime)
                  .map(java.sql.Timestamp.valueOf)
                  .map {
                    case ts if ts.getNanos % 1000 == 0 => ts
                    case _ => throw IllegalStateException("Spark does not support nanosecond resolution in 'localdatetime'")
                  }
                  .orNull

                lit(localDateTimeValue).cast(DataTypes.TimestampType)
              case None => functions.current_timestamp()
            }

          case Date(dateExpr) =>
            // TODO: Move code outside of expr mapper
            dateExpr match {
              case Some(e) =>
                val dateValue = resolveTemporalArgument(e)
                  .map(parseDate)
                  .map(java.sql.Date.valueOf)
                  .orNull

                lit(dateValue).cast(DataTypes.DateType)
              case None => functions.current_timestamp()
            }

          case Duration(durationExpr) =>
            // TODO: Move code outside of expr mapper
            val durationValue = resolveTemporalArgument(durationExpr).map {
              case Left(m) => DurationValue(m.mapValues(_.toLong)).toCalendarInterval
              case Right(s) => DurationValue.parse(s).toCalendarInterval
            }.orNull
            lit(durationValue)


          case In(lhs, rhs) if lhs.cypherType == CTNull =>
            val array = c2
            functions
              .when(size(array) === 0, FALSE_LIT)
              .otherwise(NULL_LIT)

          case In(lhs, rhs) =>
            val element = c1
            val array = c2
            functions
              .when(size(array) === 0, FALSE_LIT)
              .when(array.isNull, NULL_LIT)
              .when(element.isNull, NULL_LIT)
              .otherwise(array_contains(array, element))

          // Arithmetic
          case Add(lhs, rhs) =>
            val lhsCT = lhs.cypherType.material
            val rhsCT = rhs.cypherType.material
            lhsCT -> rhsCT match {
              case (CTList(lhInner), CTList(rhInner)) =>
                if (lhInner.material == rhInner.material || lhInner == CTVoid || rhInner == CTVoid) {
                  concat(c1, c2)
                } else {
                  throw SparkSQLMappingException(s"Lists of different inner types are not supported (${lhInner.material}, ${rhInner.material})")
                }
              case (CTList(inner), nonListType) if nonListType == inner.material || inner.material == CTVoid => concat(c1, array(c2))
              case (nonListType, CTList(inner)) if inner.material == nonListType || inner.material == CTVoid => concat(array(c1), c2)
              case (CTString, _) if rhsCT.subTypeOf(CTNumber) => concat(c1, c2.cast(StringType))
              case (_, CTString) if lhsCT.subTypeOf(CTNumber) => concat(c1.cast(StringType), c2)
              case (CTString, CTString) => concat(c1, c2)
              case (CTDate, CTDuration) => TemporalUdfs.dateAdd(c1, c2)
              case _ => c1 + c2
            }

          case Subtract(lhs, rhs) if lhs.cypherType.material.subTypeOf(CTDate) && rhs.cypherType.material.subTypeOf(CTDuration) =>
            TemporalUdfs.dateSubtract(c1, c2)

          case _: Subtract => c1 - c2

          case _: Multiply => c1 * c2
          case div: Divide => (c1 / c2).cast(div.cypherType.getSparkType)

          // Id functions
          case _: Id => c1
          case PrefixId(_, prefix) => c1.addPrefix(lit(prefix))
          case ToId(e) =>
            e.cypherType.material match {
              case CTInteger => c1.encodeLongAsCAPSId
              // TODO: Remove this call; we shouldn't have nodes or rels as concrete types here
              case _: CTNode | _: CTRelationship | CTIdentity => c1
              case other => throw IllegalArgumentException("a type that may be converted to an ID", other)
            }

          // Functions
          case _: MonotonicallyIncreasingId => functions.monotonically_increasing_id()
          case Labels(e) =>
            val possibleLabels = header.labelsFor(e.owner.get).toSeq.sortBy(_.label.name)
            val labelBooleanFlagsCol = possibleLabels.map(_.asSparkSQLExpr)
            val nodeLabels = filter_true(possibleLabels.map(_.label.name), labelBooleanFlagsCol)
            nodeLabels

          case Type(e) =>
            val possibleRelTypes = header.typesFor(e.owner.get).toSeq.sortBy(_.relType.name)
            val relTypeBooleanFlagsCol = possibleRelTypes.map(_.asSparkSQLExpr)
            val relTypes = filter_true(possibleRelTypes.map(_.relType.name), relTypeBooleanFlagsCol)
            val relType = get_array_item(relTypes, index = 0)
            relType

          case Keys(e) =>
            e.cypherType.material match {
              case _: CTNode | _: CTRelationship =>
                val possibleProperties = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
                val propertyNames = possibleProperties.map(_.key.name)
                val propertyValues = possibleProperties.map(_.asSparkSQLExpr)
                filter_not_null(propertyNames, propertyValues)

              case CTMap(inner) =>
                val mapColumn = c1
                val (propertyKeys, propertyValues) = inner.keys.map { e =>
                  // Whe have to make sure that every column has the same type (true or null)
                  e -> functions.when(mapColumn.getField(e).isNotNull, TRUE_LIT).otherwise(NULL_LIT)
                }.toSeq.unzip
                filter_not_null(propertyKeys, propertyValues)

              case other => throw IllegalArgumentException("an Expression with type CTNode, CTRelationship or CTMap", other)
            }

          case Properties(e) =>
            e.cypherType.material match {
              case _: CTNode | _: CTRelationship =>
                val propertyExpressions = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
                val propertyColumns = propertyExpressions
                  .map(propertyExpression => propertyExpression.asSparkSQLExpr.as(propertyExpression.key.name))
                createStructColumn(propertyColumns)
              case _: CTMap => c1
              case other =>
                throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
            }

          case StartNodeFunction(e) => header.startNodeFor(e.owner.get).asSparkSQLExpr
          case EndNodeFunction(e) => header.endNodeFor(e.owner.get).asSparkSQLExpr

          case _: ToFloat => c1.cast(DoubleType)
          case _: ToInteger => c1.cast(IntegerType)
          case _: ToString => c1.cast(StringType)
          case _: ToBoolean => c1.cast(BooleanType)

          case _: Trim => trim(c1)
          case _: LTrim => ltrim(c1)
          case _: RTrim => rtrim(c1)
          case _: ToUpper => upper(c1)
          case _: ToLower => lower(c1)

          case _: Range =>
            val stepCol = convertedChildren.applyOrElse(2, (_: Int) => ONE_LIT)
            functions.sequence(c1, c2, stepCol)

          case _: Replace => translate(c1, c2, c3)

          case _: Substring =>
            val lengthCol = convertedChildren.applyOrElse(2, (_: Int) => length(c1) - c2)
            c1.substr(c2 + ONE_LIT, lengthCol)

          // Mathematical functions
          case E => E_LIT
          case Pi => PI_LIT

          case _: Sqrt => functions.sqrt(c1)
          case _: Log => functions.log(c1)
          case _: Log10 => functions.log(10.0, c1)
          case _: Exp => functions.exp(c1)
          case _: Abs => functions.abs(c1)
          case _: Ceil => functions.ceil(c1).cast(DoubleType)
          case _: Floor => functions.floor(c1).cast(DoubleType)
          case Rand => functions.rand()
          case _: Round => functions.round(c1).cast(DoubleType)
          case _: Sign => functions.signum(c1).cast(IntegerType)

          case _: Acos => functions.acos(c1)
          case _: Asin => functions.asin(c1)
          case _: Atan => functions.atan(c1)
          case _: Atan2 => functions.atan2(c1, c2)
          case _: Cos => functions.cos(c1)
          case Cot(e) => Divide(IntegerLit(1), Tan(e))(CTFloat).asSparkSQLExpr
          case _: Degrees => functions.degrees(c1)
          case Haversin(e) => Divide(Subtract(IntegerLit(1), Cos(e))(CTFloat), IntegerLit(2))(CTFloat).asSparkSQLExpr
          case _: Radians => functions.radians(c1)
          case _: Sin => functions.sin(c1)
          case _: Tan => functions.tan(c1)

          // Time functions

          case Timestamp => functions.current_timestamp().cast(LongType)

          // Bit operations

          case _: BitwiseAnd => c1.bitwiseAND(c2)
          case _: BitwiseOr => c1.bitwiseOR(c2)
          case ShiftLeft(_, IntegerLit(shiftBits)) => shiftLeft(c1, shiftBits.toInt)
          case ShiftRightUnsigned(_, IntegerLit(shiftBits)) => shiftRightUnsigned(c1, shiftBits.toInt)

          // Pattern Predicate
          case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

          case Coalesce(es) =>
            val columns = es.map(_.asSparkSQLExpr)
            functions.coalesce(columns: _*)

          case CaseExpr(alternatives, default) =>
            val convertedAlternatives = alternatives.map {
              case (predicate, action) => functions.when(predicate.asSparkSQLExpr, action.asSparkSQLExpr)
            }

            val alternativesWithDefault = default match {
              case Some(inner) => convertedAlternatives :+ inner.asSparkSQLExpr
              case None => convertedAlternatives
            }

            val reversedColumns = alternativesWithDefault.reverse

            val caseColumn = reversedColumns.tail.foldLeft(reversedColumns.head) {
              case (tmpCol, whenCol) => whenCol.otherwise(tmpCol)
            }
            caseColumn

          case ContainerIndex(container, index) =>
            val indexCol = index.asSparkSQLExpr
            val containerCol = container.asSparkSQLExpr

            container.cypherType.material match {
              case _: CTList | _: CTMap => containerCol.get(indexCol)
              case other => throw NotImplementedException(s"Accessing $other by index is not supported")
            }

          case MapExpression(items) => expr.cypherType.material match {
            case CTMap(_) =>
              val innerColumns = items.map {
                case (key, innerExpr) => innerExpr.asSparkSQLExpr.as(key)
              }.toSeq
              createStructColumn(innerColumns)
            case other => throw IllegalArgumentException("an expression of type CTMap", other)
          }

            // Aggregators
          case Count(_, distinct) =>
            if (distinct) countDistinct(c1)
            else count(c1)

          case Collect(_, distinct) =>
            if (distinct) collect_set(c1)
            else collect_list(c1)

          case CountStar => count(ONE_LIT)
          case _: Avg => avg(c1)//.cast(cypherType.getSparkType)
          case _: Max => max(c1)
          case _: Min => min(c1)
          case _: Sum => sum(c1)

          case _ =>
            throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
        }
      }
    }
  }

  private def columnFor(expr: Expr)(implicit header: RecordHeader, df: DataFrame): Column = {
    val columnName = header.getColumn(expr).getOrElse(throw IllegalArgumentException(
      expected = s"Expression in ${header.expressions.mkString("[", ", ", "]")}",
      actual = expr)
    )
    if (df.columns.contains(columnName)) {
      df.col(columnName)
    } else {
      NULL_LIT
    }
  }

  private def toSparkLiteral(value: Any): Column = value match {
    case list: List[_] => array(list.map(lit): _*)
    case map: Map[_, _] =>
      val columns = map.map {
        case (key, v) => toSparkLiteral(v).as(key.toString)
      }.toSeq
      createStructColumn(columns)
    case _ => lit(value)
  }

  private def createStructColumn(structColumns: Seq[Column]): Column = {
    if (structColumns.isEmpty) {
      val emptyStructUdf = functions.udf( () => new GenericRowWithSchema(Array(), StructType(Nil)), StructType(Nil))
      emptyStructUdf()
    } else {
      functions.struct(structColumns: _*)
    }
  }

  private def convert(expr: Expr)(ifNotNull: Seq[Column] => Column)
    (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
    val evaluatedArgs = expr.children.map(_.asSparkSQLExpr)
    // TODO: Cleaner compatibility check
    expr.cypherType.getSparkType
    if (expr.cypherType == CTNull) {
      NULL_LIT
    } else if (expr.children.nonEmpty && expr.nullInNullOut && expr.cypherType.isNullable) {
      val nullPropagationCases = evaluatedArgs.map(_.isNull.expr).zip(Seq.fill(evaluatedArgs.length)(NULL_LIT.expr))
      new Column(CaseWhen(nullPropagationCases, ifNotNull(evaluatedArgs).expr))
    } else {
      new Column(ifNotNull(evaluatedArgs).expr)
    }
  }

}
