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
package org.opencypher.caps.api.exception

import org.opencypher.caps.api.exception.CypherException.{ErrorDetails, ErrorPhase, ErrorType}

/**
  * Cypher exceptions are thrown due to the reasons specified by the Cypher TCK and follow the specified format.
  */
abstract class CypherException(errorType: ErrorType, phase: ErrorPhase, detail: ErrorDetails) extends RuntimeException(s"$errorType during $phase. Details: $detail") with Serializable

object CypherException {

  sealed trait ErrorType

  /**
    * Possible error types as specified by the TCK.
    */
  object ErrorType {

    object SyntaxError extends ErrorType

    object ParameterMissing extends ErrorType

    object ConstraintVerificationFailed extends ErrorType

    //TODO: Validation/verification are too similar. Fix in TCK?
    object ConstraintValidationFailed extends ErrorType

    object EntityNotFound extends ErrorType

    object PropertyNotFound extends ErrorType

    object LabelNotFound extends ErrorType

    object TypeError extends ErrorType

    object ArgumentError extends ErrorType

    object ArithmeticError extends ErrorType

    object ProcedureError extends ErrorType

  }

  sealed trait ErrorPhase

  /**
    * Possible error phases as specified by the TCK.
    */
  object ErrorPhase {

    object Runtime extends ErrorPhase

    object CompileTime extends ErrorPhase

  }

  sealed trait ErrorDetails

  /**
    * Possible error details as specified by the TCK.
    */
  object ErrorDetails {

    object InvalidElementAccess extends ErrorDetails

    object MapElementAccessByNonString extends ErrorDetails

    object ListElementAccessByNonInteger extends ErrorDetails

    object NestedAggregation extends ErrorDetails

    object NegativeIntegerArgument extends ErrorDetails

    object DeleteConnectedNode extends ErrorDetails

    object RequiresDirectedRelationship extends ErrorDetails

    object InvalidRelationshipPattern extends ErrorDetails

    object VariableAlreadyBound extends ErrorDetails

    object InvalidArgumentType extends ErrorDetails

    object InvalidArgumentValue extends ErrorDetails

    object NumberOutOfRange extends ErrorDetails

    object UndefinedVariable extends ErrorDetails

    object VariableTypeConflict extends ErrorDetails

    object RelationshipUniquenessViolation extends ErrorDetails

    object CreatingVarLength extends ErrorDetails

    object InvalidParameterUse extends ErrorDetails

    object InvalidClauseComposition extends ErrorDetails

    object FloatingPointOverflow extends ErrorDetails

    object PropertyAccessOnNonMap extends ErrorDetails

    object InvalidArgumentExpression extends ErrorDetails

    object InvalidUnicodeCharacter extends ErrorDetails

    object NonConstantExpression extends ErrorDetails

    object NoSingleRelationshipType extends ErrorDetails

    object InvalidAggregation extends ErrorDetails

    object UnknownFunction extends ErrorDetails

    object InvalidNumberLiteral extends ErrorDetails

    object InvalidUnicodeLiteral extends ErrorDetails

    object MergeReadOwnWrites extends ErrorDetails

    object NoExpressionAlias extends ErrorDetails

    object DifferentColumnsInUnion extends ErrorDetails

    object InvalidDelete extends ErrorDetails

    object InvalidPropertyType extends ErrorDetails

    object ColumnNameConflict extends ErrorDetails

    object NoVariablesInScope extends ErrorDetails

    object DeletedEntityAccess extends ErrorDetails

    object InvalidArgumentPassingMode extends ErrorDetails

    object InvalidNumberOfArguments extends ErrorDetails

    object MissingParameter extends ErrorDetails

    object ProcedureNotFound extends ErrorDetails

  }

}
