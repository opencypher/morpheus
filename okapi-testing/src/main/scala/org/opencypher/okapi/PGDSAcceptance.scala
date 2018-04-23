package org.opencypher.okapi

import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.propertygraph.{TestNode, TestRelationship}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

abstract class PGDSAcceptance extends FunSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = create(???, ???)

  override def afterAll(): Unit = super.afterAll()

  def create(
    nodes: Set[TestNode],
    relationships: Set[TestRelationship]
  ): PropertyGraphDataSource

}
