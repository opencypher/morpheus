### `morpheus-spark-cypher-%RELEASE_VERSION%`

#### Release notes

<!--put release notes here-->

#### Using Morpheus in your system

The artifact is released to [Maven Central](https://search.maven.org/#artifactdetails%7Corg.opencypher%7Cmorpheus-spark-cypher%7C%RELEASE_VERSION%%7Cjar).
To use it in a Maven project, add the following dependency to your pom:

```
<dependency>
  <groupId>org.opencypher</groupId>
  <artifactId>morpheus-spark-cypher</artifactId>
  <version>%RELEASE_VERSION%</version>
</dependency>
```

For SBT:
```
libraryDependencies += "org.opencypher" % "morpheus-spark-cypher" % "%RELEASE_VERSION%"
```

### `morpheus-spark-cypher-%RELEASE_VERSION%-all`
This is a fat jar that does not include the Spark dependencies. It is intended to be used in environments where Spark is already present, for example, a Spark cluster or a notebook.
