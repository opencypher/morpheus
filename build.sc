import $file.shared
import mill._

object OkapiTrees extends shared.OkapiModule {

  object test extends OkapiTests

}

object OkapiApi extends shared.OkapiModule {

  override def moduleDeps = Seq(OkapiTrees)

  override def ivyDeps = super.ivyDeps() ++ Agg(cats)

  object test extends OkapiTests {
    override def ivyDeps = super.ivyDeps() ++ Agg(mockito)
  }

}

object OkapiTesting extends shared.OkapiModule {

  override def moduleDeps = Seq(OkapiApi)

  override def ivyDeps = super.ivyDeps() ++ Agg(
    scalaTest,
    frontend,
    neo4jHarness
  )

  object test extends OkapiTests

}

object OkapiIr extends shared.OkapiModule {

  override def moduleDeps = Seq(OkapiApi)

  override def scalacOptions = super.scalacOptions() ++ Seq(
    //    "-Ypartial-unification",
    //    "-unchecked",
    //    "-deprecation",
    //    "-feature",
    //    "-Xfatal-warnings",
    //    "-Xfuture",
    //    "-Ywarn-adapted-args",
    //    "-Yopt-warnings:at-inline-failed",
    //    "-Yopt:l:project",
    "-Ypartial-unification"
  )

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
