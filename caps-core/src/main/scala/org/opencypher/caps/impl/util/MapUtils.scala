/*
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
package org.opencypher.caps.impl.util

trait MapUtils {

  def merge[A, B](a: Map[A, B], b: Map[A, B])(mergeValues: (B, B) => B): Map[A, B] =
    (a.keySet ++ b.keySet)
      .map(k => (k, a.get(k), b.get(k)))
      .collect {
        case (k, Some(v1), Some(v2)) => k -> mergeValues(v1, v2)
        case (k, Some(v1), _) => k -> v1
        case (k, _, Some(v2)) => k -> v2
      }.toMap
}

object MapUtils extends MapUtils
