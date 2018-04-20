import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._
import io.circe._
import com.softwaremill.sttp._

object Hello extends App {

  val parser = new scopt.OptionParser[Config]("Github Release") {
    opt[String]('v',"releaseVersion")
      .required()
      .action((version, config) => config.copy(releaseVersion = version))
      .text("The version for which a GitHub release is created.")

    opt[String]('b',"branch")
      .action((branch, config) => config.copy(branch = branch))
      .text("The branch for which the tag is created. Default is `master`.")

    opt[String]('t',"githubToken")
      .required()
      .action((token, config) => config.copy(token = token))
      .text("GitHub API token.")

    opt[String]('c',"capsRoot")
      .required()
      .action((path, config) => config.copy(capsRoot = path))
      .text("Path of the CAPS root folder.")
  }

  parser.parse(args, Config()) match {
    case Some(c) => runScript(c)
    case _ => ()
  }

  private def runScript(config: Config) = {
    val releaseVersion = config.releaseVersion
    val branch = config.branch
    val releaseName = s"CAPS Release Version $releaseVersion"
    val releaseBody =
      s"""
         |### `spark-cypher-$releaseVersion`
         |
         |This is the artifact deployed to [Maven Central](https://search.maven.org/#artifactdetails%7Corg.opencypher%7Cspark-cypher%7C$releaseVersion%7Cjar). To use this release in a Maven project, add the following dependency to your pom:
         |```
         |<dependency>
         |  <groupId>org.opencypher</groupId>
         |  <artifactId>spark-cypher</artifactId>
         |  <version>$releaseVersion</version>
         |</dependency>
         |```
         |
         |For SBT:
         |```
         |libraryDependencies += "org.opencypher" % "spark-cypher" % "$releaseVersion"
         |```
         |
         |### `spark-cypher-$releaseVersion-cluster`
         |This is a fat jar that does not include the Spark dependencies. It is intended to be used in environments where Spark is already present, for example, a Spark cluster or a notebook.
       """.stripMargin
    val draft = false
    val preRelease = releaseVersion.contains("-")

    val requestBody = GithubReleaseCreationRequest(
      releaseVersion,
      branch,
      releaseName,
      releaseBody,
      draft,
      preRelease
    )

    implicit val backend = HttpURLConnectionBackend()
    val requestJson = requestBody.asJson
    val requestUri = new java.net.URI(s"https://api.github.com/repos/opencypher/cypher-for-apache-spark/releases?access_token=${config.token}")
    val request = sttp
      .body(requestJson.toString())
      .post(Uri(requestUri))

    println(s"Sending creation request: $request")
    val maybeResponse = request.send()
    println(s"Response to creation request: $maybeResponse")

    val responseBodyString = maybeResponse.body.right.get
    val responseBodyJson = parse(responseBodyString).right.get
    val releaseId = responseBodyJson.asObject.get("id").get.toString

    val targetPath = s"${config.capsRoot}/spark-cypher/target/artifacts/"
    val clusterJarAsset = s"spark-cypher-$releaseVersion-cluster.jar"
    val clusterJarPath = targetPath + clusterJarAsset
    val standaloneJarAsset = s"spark-cypher-$releaseVersion.jar"
    val standaloneJarPath = targetPath + standaloneJarAsset

    def uploadAsset(releaseId: String, assetName: String, filePath: String) = {
      val requestUri = new java.net.URI(s"https://uploads.github.com/repos/opencypher/cypher-for-apache-spark/releases/$releaseId/assets?access_token=${config.token}&name=$assetName")
      val request = sttp
        .multipartBody(multipart("file_part", new java.io.File(filePath)))
        .contentType("application/java-archive")
        .post(Uri(requestUri))

      println(s"Sending upload request: $request")
      val maybeResponse = request.send()
      println(s"Response to upload request: $maybeResponse")
    }

    uploadAsset(releaseId, clusterJarAsset, clusterJarPath)
    uploadAsset(releaseId, standaloneJarAsset, standaloneJarPath)
  }
}

case class Config(
  releaseVersion: String = "",
  branch: String = "master",
  token: String = "",
  capsRoot: String = ""
)

case class GithubReleaseCreationRequest(
  tag_name: String,
  target_commitish: String,
  name: String,
  body: String,
  draft: Boolean,
  prerelease: Boolean
)
