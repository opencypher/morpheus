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
package org.opencypher.okapi.impl.exception

/**
  * Exceptions that are not covered by the TCK. They are related to limitations of a specific Cypher implementation
  * or to session or property graph interactions that are not covered by the TCK.
  */
//TODO: Either: 1. Convert to CypherException; 2. Create categories that makes sense in the API module for them; or 3. Move to the internals of the system to which they belong.
abstract class InternalException(msg: String) extends RuntimeException(msg) with Serializable

final case class SchemaException(msg: String) extends InternalException(msg)

final case class CypherValueException(msg: String) extends InternalException(msg)

final case class NotImplementedException(msg: String) extends InternalException(msg)

final case class IllegalStateException(msg: String) extends InternalException(msg)

final case class IllegalArgumentException(expected: Any, actual: Any = "none")
  extends InternalException(s"\nExpected:\n\t$expected\nFound:\n\t$actual")

final case class UnsupportedOperationException(msg: String) extends InternalException(msg)

final case class GraphNotFoundException(msg: String) extends InternalException(msg)
