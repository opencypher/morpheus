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

abstract class CypherException(msg: String) extends RuntimeException(msg) with Serializable

final case class SchemaException(msg: String) extends CypherException(msg)

final case class CypherValueException(msg: String) extends CypherException(msg)

final case class NotImplementedException(msg: String) extends CypherException(msg)

final case class IllegalStateException(msg: String) extends CypherException(msg)

final case class IllegalArgumentException(expected: Any, actual: Any = "none")
  extends CypherException(s"\nExpected:\n\t$expected\nFound:\n\t$actual")

final case class UnsupportedOperationException(msg: String) extends CypherException(msg)
