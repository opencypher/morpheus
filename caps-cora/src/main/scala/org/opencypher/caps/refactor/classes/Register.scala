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
package org.opencypher.caps.refactor.classes

// Manage collections of elements of type Def such that the collection holds at most one element
// with a key of type Key.
//
// Elements may be addressed efficiently using references of type Ref
//
trait Register[Collection] {

  self =>

  type Ref
  type Key
  type Def

  def empty: Collection
  def contents(collection: Collection): Traversable[(Ref, Def)]

  def key(defn: Def): Key

  def find(collection: Collection, defn: Def): Option[Ref]
  def findByKey(collection: Collection, key: Key): Option[Ref]
  def lookup(collection: Collection, ref: Ref): Option[Def]

  // left if key(defn) is already inserted at different ref, right otherwise
  def update(collection: Collection, ref: Ref, defn: Def): Either[Ref, Collection]

  // left if key(defn) is already inserted with a different defn, right otherwise
  def insert(collection: Collection, defn: Def): Either[Ref, (Option[Collection], Ref)]

  // none if not contained. All previous refs are invalid after a remove!
  def remove(collection: Collection, ref: Ref): Option[Collection]
}

object Register {
  @inline
  final def apply[C, R, K, D](implicit register: Register[C] { type Ref = R; type Key = K; type Def = D })
    : Register[C] {
      type Ref = R
      type Key = K
      type Def = D
    } = register
}
