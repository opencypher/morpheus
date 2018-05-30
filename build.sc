import $file.shared
import shared.OkapiModule
import mill._

object OkapiTrees extends OkapiModule {

  object test extends OkapiTests

}

object OkapiApi extends OkapiModule {

  override def moduleDeps = Seq(OkapiTrees)

  override def ivyDeps = super.ivyDeps() ++ Agg(
    cats,
    uPickle
  )

  object test extends OkapiTests {
    override def ivyDeps = super.ivyDeps() ++ Agg(mockito)
  }

}

object OkapiTesting extends OkapiModule {

  override def moduleDeps = Seq(OkapiApi)

  override def ivyDeps = super.ivyDeps() ++ Agg(
    scalaTest,
    frontend,
    neo4jHarness
  )

  object test extends OkapiTests

}

object OkapiIr extends OkapiModule {

  override def moduleDeps = Seq(OkapiApi)

  override def scalacOptions = super.scalacOptions() ++ Seq("-Ypartial-unification")

  override def ivyDeps = super.ivyDeps() ++ Agg(
    frontend,
    cats,
    eff
  )

  object test extends OkapiTests {
    override def moduleDeps = super.moduleDeps ++ Seq(OkapiTesting)

    override def ivyDeps = super.ivyDeps() ++ Agg(
      mockito,
      cypherTesting,
      astTesting,
      frontendTesting
    )
  }

}

object OkapiLogical extends OkapiModule {

  override def moduleDeps = Seq(OkapiIr)

  object test extends OkapiTests {
    override def moduleDeps = super.moduleDeps ++ Seq(OkapiTesting, OkapiIr.test)
  }

}

object OkapiRelational extends OkapiModule {

  override def moduleDeps = Seq(OkapiLogical)

  override def scalacOptions = super.scalacOptions() ++ Seq("-Ypartial-unification")

  object test extends OkapiTests {
    override def moduleDeps = super.moduleDeps ++ Seq(OkapiTesting, OkapiIr.test)

    override def ivyDeps = super.ivyDeps() ++ Agg(
    )
  }

}

object SparkCypher extends OkapiModule {

  override def moduleDeps = Seq(OkapiRelational)

  override def ivyDeps = super.ivyDeps() ++ Agg(
    spark,
    sparkSql,
    sparkCatalyst,
    neo4jDriver,
    circeCore
  )

}
