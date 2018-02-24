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
// TODO: Make sure all Cypher-related exceptions we throw are instances of this
// TODO: Make sure all other exceptions are instances of another trait in this package
abstract class CypherException(errorType: ErrorType, phase: ErrorPhase, detail: ErrorDetails) extends RuntimeException(s"$errorType during $phase. Details: $detail") with Serializable

object CypherException {

  sealed trait ErrorType

  /**
    * Possible error types as specified by the TCK.
    */
  object ErrorType {

    case object SyntaxError extends ErrorType

    case object ParameterMissing extends ErrorType

    case object ConstraintVerificationFailed extends ErrorType

    //TODO: Validation/verification are too similar. Fix in TCK?
    case object ConstraintValidationFailed extends ErrorType

    case object EntityNotFound extends ErrorType

    case object PropertyNotFound extends ErrorType

    case object LabelNotFound extends ErrorType

    case object TypeError extends ErrorType

    case object ArgumentError extends ErrorType

    case object ArithmeticError extends ErrorType

    case object ProcedureError extends ErrorType

  }

  sealed trait ErrorPhase

  /**
    * Possible error phases as specified by the TCK.
    */
  object ErrorPhase {

    case object Runtime extends ErrorPhase

    case object CompileTime extends ErrorPhase

  }

  sealed trait ErrorDetails

  /**
    * Possible error details as specified by the TCK.
    */
  object ErrorDetails {

    case object InvalidElementAccess extends ErrorDetails

    case object MapElementAccessByNonString extends ErrorDetails

    case object ListElementAccessByNonInteger extends ErrorDetails

    case object NestedAggregation extends ErrorDetails

    case object NegativeIntegerArgument extends ErrorDetails

    case object DeleteConnectedNode extends ErrorDetails

    case object RequiresDirectedRelationship extends ErrorDetails

    case object InvalidRelationshipPattern extends ErrorDetails

    case object VariableAlreadyBound extends ErrorDetails

    case object InvalidArgumentType extends ErrorDetails

    case object InvalidArgumentValue extends ErrorDetails

    case object NumberOutOfRange extends ErrorDetails

    case object UndefinedVariable extends ErrorDetails

    case object VariableTypeConflict extends ErrorDetails

    case object RelationshipUniquenessViolation extends ErrorDetails

    case object CreatingVarLength extends ErrorDetails

    case object InvalidParameterUse extends ErrorDetails

    case object InvalidClauseComposition extends ErrorDetails

    case object FloatingPointOverflow extends ErrorDetails

    case object PropertyAccessOnNonMap extends ErrorDetails

    case object InvalidArgumentExpression extends ErrorDetails

    case object InvalidUnicodeCharacter extends ErrorDetails

    case object NonConstantExpression extends ErrorDetails

    case object NoSingleRelationshipType extends ErrorDetails

    case object InvalidAggregation extends ErrorDetails

    case object UnknownFunction extends ErrorDetails

    case object InvalidNumberLiteral extends ErrorDetails

    case object InvalidUnicodeLiteral extends ErrorDetails

    case object MergeReadOwnWrites extends ErrorDetails

    case object NoExpressionAlias extends ErrorDetails

    case object DifferentColumnsInUnion extends ErrorDetails

    case object InvalidDelete extends ErrorDetails

    case object InvalidPropertyType extends ErrorDetails

    case object ColumnNameConflict extends ErrorDetails

    case object NoVariablesInScope extends ErrorDetails

    case object DeletedEntityAccess extends ErrorDetails

    case object InvalidArgumentPassingMode extends ErrorDetails

    case object InvalidNumberOfArguments extends ErrorDetails

    case object MissingParameter extends ErrorDetails

    case object ProcedureNotFound extends ErrorDetails

  }
}
