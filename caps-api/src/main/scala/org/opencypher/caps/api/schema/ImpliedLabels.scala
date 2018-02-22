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
package org.opencypher.caps.api.schema

// TODO: Move to impl package
object ImpliedLabels {
  val empty: ImpliedLabels = ImpliedLabels(Map.empty)
}

// TODO: Move to impl package
case class ImpliedLabels(m: Map[String, Set[String]]) {

  def transitiveImplicationsFor(known: Set[String]): Set[String] = {
    val next = known.flatMap(implicationsFor)
    if (next == known) known else transitiveImplicationsFor(next)
  }

  def withImplication(source: String, target: String): ImpliedLabels = {
    val implied = implicationsFor(source)
    if (implied(target)) this else copy(m = m.updated(source, implied + target))
  }

  def toPairs: Set[(String, String)] = {
    m.toArray
      .flatMap(pair => pair._2.map(elem => (pair._1, elem)))
      .toSet
  }

  def filterByLabels(labels: Set[String]): ImpliedLabels = {
    val filteredImplications = m.collect {
      case (k, v) if labels.contains(k) => k -> v.intersect(labels)
    }

    ImpliedLabels(filteredImplications)
  }

  private def implicationsFor(source: String) = m.getOrElse(source, Set.empty) + source
}
