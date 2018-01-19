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
package org.opencypher.caps.refactor.syntax

import org.opencypher.caps.refactor.classes.Register

import scala.language.implicitConversions

object RegisterSyntax extends RegisterSyntax

trait RegisterSyntax {
  def key[D, K](defn: D)(implicit register: Register[_] { type Def = D; type Key = K }): register.Key =
    register.key(defn)

  implicit def registerSyntax[C, R, K, D](coll: C)(
      implicit
      register: Register[C] { type Ref = R; type Key = K; type Def = D }): RegisterOps[C, R, K, D] =
    new RegisterOps[C, R, K, D](coll)
}

final class RegisterOps[C, R, K, D](coll: C)(
    implicit
    val register: Register[C] { type Ref = R; type Key = K; type Def = D }) {
  def contents: Traversable[(R, D)] = register.contents(coll)

  def lookup(ref: R): Option[D] = register.lookup(coll, ref)
  def find(defn: D): Option[R] = register.find(coll, defn)
  def findByKey(key: K): Option[R] = register.findByKey(coll, key)
  def insert(defn: D): Either[R, (Option[C], R)] = register.insert(coll, defn)
  def update(ref: R, defn: D): Either[R, C] = register.update(coll, ref, defn)
  def remove(ref: R): Option[C] = register.remove(coll, ref)
}
