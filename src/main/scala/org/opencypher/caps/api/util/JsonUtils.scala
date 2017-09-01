/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.util

import io.circe.Decoder
import io.circe.parser.parse

object JsonUtils {
  def parseJson[T](jsonString: String)(implicit decoder: Decoder[T]): T = {
    parse(jsonString) match {
      case Left(failure) => throw new RuntimeException(s"Invalid json file: $failure")
      case Right(json) => json.hcursor.as[T] match {
        case Left(failure) => {
          val msg = s"Invalid JSON schema: Could not find mandatory element '${failure.history.head.productElement(0)}'"
          throw new RuntimeException(msg)
        }
        case Right(elem) => elem
      }
    }
  }
}
