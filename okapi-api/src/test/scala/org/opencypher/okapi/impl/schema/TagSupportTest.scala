package org.opencypher.okapi.impl.schema

import org.opencypher.okapi.api.schema.Schema
import TagSupport._
import org.opencypher.okapi.test.BaseTestSuite

class TagSupportTest extends BaseTestSuite {

  it("allows adding tags to a schema") {
    val taggedSchema = Schema.empty.withTags(1, 2, 3)
    taggedSchema.tags should equal(Set(1, 2, 3))
  }

  it("preserves other schema entries when adding tags") {
    val taggedSchema = Schema.empty.withNodePropertyKeys(Set("a")).withTags(1, 2, 3)
    taggedSchema.tags should equal(Set(1, 2, 3))
    taggedSchema.labels should equal(Set("a"))
  }

  it("merges tags when concatenating schemas") {
    val s1: Schema with TagSupport = Schema.empty.withTags(1, 2, 3)
    val s2: Schema with TagSupport = Schema.empty.withTags(1, 2, 4)
    val concatenatedSchema = s1 ++ s2
    concatenatedSchema should equal(Schema.empty.withTags(1, 2, 3, 4))
  }

  it("replaces tags") {
    val taggedSchema = Schema.empty.withTags(1, 2, 3).replaceTags(Map(2 -> 5))
    taggedSchema should equal(Schema.empty.withTags(1, 5, 3))
  }

  it("replaces tags on the rhs schema when resolving tag conflicts in a schema union") {
    val s1: Schema with TagSupport = Schema.empty.withTags(1, 2, 3)
    val s2: Schema with TagSupport = Schema.empty.withTags(1, 2, 4)
    val schemaUnion = s1 union s2
    schemaUnion.tags.size should equal(6)
    Set(1, 2, 3).subsetOf(schemaUnion.tags) should equal(true)
  }

}
