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
package org.opencypher.caps.api.ir.global

import org.opencypher.caps.impl.util.RefCollection.AbstractRegister

sealed trait Global extends Any {
  def name: String
}

object Global {

  implicit val labelRegister = new AbstractRegister[LabelRef, String, Label] {
    override protected def id(ref: LabelRef): Int = ref.id
    override protected def ref(id: Int): LabelRef = LabelRef(id)
    override def key(defn: Label): String = defn.name
  }

  implicit val relTypeRegister = new AbstractRegister[RelTypeRef, String, RelType] {
    override protected def id(ref: RelTypeRef): Int = ref.id
    override protected def ref(id: Int): RelTypeRef = RelTypeRef(id)
    override def key(defn: RelType): String = defn.name
  }

  implicit val pkRegister = new AbstractRegister[PropertyKeyRef, String, PropertyKey] {
    override protected def id(ref: PropertyKeyRef): Int = ref.id
    override protected def ref(id: Int): PropertyKeyRef = PropertyKeyRef(id)
    override def key(defn: PropertyKey): String = defn.name
  }

  implicit val constRegister = new AbstractRegister[ConstantRef, String, Constant] {
    override protected def id(ref: ConstantRef): Int = ref.id
    override protected def ref(id: Int): ConstantRef = ConstantRef(id)
    override def key(defn: Constant): String = defn.name
  }
}

sealed trait GlobalRef[D <: Global] extends Any {
  def id: Int
}

final case class Label(name: String) extends AnyVal with Global
final case class LabelRef(id: Int) extends AnyVal with GlobalRef[Label]

final case class PropertyKey(name: String) extends AnyVal with Global
final case class PropertyKeyRef(id: Int) extends AnyVal with GlobalRef[PropertyKey]

final case class RelType(name: String) extends AnyVal with Global
final case class RelTypeRef(id: Int) extends AnyVal with GlobalRef[RelType]

final case class Constant(name: String) extends AnyVal with Global
final case class ConstantRef(id: Int) extends AnyVal with GlobalRef[Constant]



