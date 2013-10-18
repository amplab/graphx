/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
// For Sonatype publishing
//import com.jsuereth.pgp.sbtplugin.PgpKeys._

object SparkBuild extends Build {
  // Hadoop version to build against. For example, "1.0.4" for Apache releases, or
  // "2.0.0-mr1-cdh4.2.0" for Cloudera Hadoop. Note that these variables can be set
  // through the environment variables SPARK_HADOOP_VERSION and SPARK_YARN.
  val DEFAULT_HADOOP_VERSION = "1.0.4"
  val DEFAULT_YARN = false

  // HBase version; set as appropriate.
  val HBASE_VERSION = "0.94.6"

  // Target JVM version
  val SCALAC_JVM_VERSION = "jvm-1.5"
  val JAVAC_JVM_VERSION = "1.5"

  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(allProjects: _*)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val repl = Project("repl", file("repl"), settings = replSettings)
    .dependsOn(core, graph, bagel, mllib)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings)
    .dependsOn(core, mllib, graph, bagel, streaming)

  lazy val tools = Project("tools", file("tools"), settings = toolsSettings) dependsOn(core) dependsOn(streaming)

  lazy val bagel = Project("bagel", file("bagel"), settings = bagelSettings) dependsOn(core)

  lazy val graph = Project("graph", file("graph"), settings = graphSettings) dependsOn(core) 

  lazy val streaming = Project("streaming", file("streaming"), settings = streamingSettings) dependsOn(core)

  lazy val mllib = Project("mllib", file("mllib"), settings = mllibSettings) dependsOn(core)

  lazy val yarn = Project("yarn", file("yarn"), settings = yarnSettings) dependsOn(core)

  lazy val assemblyProj = Project("assembly", file("assembly"), settings = assemblyProjSettings)
    .dependsOn(core, graph, bagel, mllib, repl, streaming) dependsOn(maybeYarn: _*)

  // A configuration to set an alternative publishLocalConfiguration
  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  // Allows build configuration to be set through environment variables
  lazy val hadoopVersion = scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)
  lazy val isYarnEnabled = scala.util.Properties.envOrNone("SPARK_YARN") match {
    case None => DEFAULT_YARN
    case Some(v) => v.toBoolean
  }

  // Conditionally include the yarn sub-project
  lazy val maybeYarn = if(isYarnEnabled) Seq[ClasspathDependency](yarn) else Seq[ClasspathDependency]()
  lazy val maybeYarnRef = if(isYarnEnabled) Seq[ProjectReference](yarn) else Seq[ProjectReference]()
  lazy val allProjects = Seq[ProjectReference](
    core, repl, examples, graph, bagel, streaming, mllib, tools, assemblyProj) ++ maybeYarnRef

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.apache.spark",
    version := "0.9.0-incubating-SNAPSHOT",
    scalaVersion := "2.9.3",
    scalacOptions := Seq("-Xmax-classfile-name", "120", "-unchecked", "-deprecation",
      "-target:" + SCALAC_JVM_VERSION),
    javacOptions := Seq("-target", JAVAC_JVM_VERSION, "-source", JAVAC_JVM_VERSION),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

    // Fork new JVMs for tests and set Java options for those
    fork := true,
    javaOptions += "-Xmx3g",

    // Only allow one test at a time, even across projects, since they run in the same JVM
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

    // also check the local Maven repository ~/.m2
    resolvers ++= Seq(Resolver.file("Local Maven Repo", file(Path.userHome + "/.m2/repository"))),

    // Shared between both core and streaming.
    resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/"),

    // For Sonatype publishing
    resolvers ++= Seq("sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "sonatype-staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"),

    publishMavenStyle := true,

    //useGpg in Global := true,

    pomExtra := (
      <parent>
        <groupId>org.apache</groupId>
        <artifactId>apache</artifactId>
        <version>13</version>
      </parent>
      <url>http://spark.incubator.apache.org/</url>
      <licenses>
        <license>
          <name>Apache 2.0 License</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:git@github.com:apache/incubator-spark.git</connection>
        <url>scm:git:git@github.com:apache/incubator-spark.git</url>
      </scm>
      <developers>
        <developer>
          <id>matei</id>
          <name>Matei Zaharia</name>
          <email>matei.zaharia@gmail.com</email>
          <url>http://www.cs.berkeley.edu/~matei</url>
          <organization>Apache Software Foundation</organization>
          <organizationUrl>http://spark.incubator.apache.org</organizationUrl>
        </developer>
      </developers>
      <issueManagement>
        <system>JIRA</system>
        <url>https://spark-project.atlassian.net/browse/SPARK</url>
      </issueManagement>
    ),

/*
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-staging"  at nexus + "service/local/staging/deploy/maven2")
    },

*/


    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "com.novocode" % "junit-interface" % "0.9" % "test",
      "org.easymock" % "easymock" % "3.1" % "test"
    ),
    /* Workaround for issue #206 (fixed after SBT 0.11.0) */
    watchTransitiveSources <<= Defaults.inDependencies[Task[Seq[File]]](watchSources.task,
      const(std.TaskExtra.constant(Nil)), aggregate = true, includeRoot = true) apply { _.join.map(_.flatten) },

    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn
  ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

  val slf4jVersion = "1.7.2"

  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeAsm = ExclusionRule(organization = "asm")
  val excludeSnappy = ExclusionRule(organization = "org.xerial.snappy")

  def coreSettings = sharedSettings ++ Seq(
    name := "spark-core",
    resolvers ++= Seq(
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
    ),

    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "14.0.1",
      "com.google.code.findbugs" % "jsr305" % "1.3.9",
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-daemon" % "commons-daemon" % "1.0.10",  // workaround for bug HADOOP-9407
      "com.ning" % "compress-lzf" % "0.8.4",
      "org.xerial.snappy" % "snappy-java" % "1.0.5",
      "org.ow2.asm" % "asm" % "4.0",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "com.typesafe.akka" % "akka-actor" % "2.0.5" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-remote" % "2.0.5" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-slf4j" % "2.0.5" excludeAll(excludeNetty),
      "it.unimi.dsi" % "fastutil" % "6.4.4",
      "colt" % "colt" % "1.2.0",
      "net.liftweb" % "lift-json_2.9.2" % "2.5",
      "org.apache.mesos" % "mesos" % "0.13.0",
      "io.netty" % "netty-all" % "4.0.0.Beta2",
      "org.apache.derby" % "derby" % "10.4.2.0" % "test",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm, excludeCglib),
      "net.java.dev.jets3t" % "jets3t" % "0.7.1",
      "org.apache.avro" % "avro" % "1.7.4",
      "org.apache.avro" % "avro-ipc" % "1.7.4" excludeAll(excludeNetty),
      "org.apache.zookeeper" % "zookeeper" % "3.4.5" excludeAll(excludeNetty),
      "com.codahale.metrics" % "metrics-core" % "3.0.0",
      "com.codahale.metrics" % "metrics-jvm" % "3.0.0",
      "com.codahale.metrics" % "metrics-json" % "3.0.0",
      "com.codahale.metrics" % "metrics-ganglia" % "3.0.0",
      "com.twitter" % "chill_2.9.3" % "0.3.1",
      "com.twitter" % "chill-java" % "0.3.1"
    )
  )

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def replSettings = sharedSettings ++ Seq(
    name := "spark-repl",
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
  )

  def examplesSettings = sharedSettings ++ Seq(
    name := "spark-examples",
    libraryDependencies ++= Seq(
      "com.twitter" % "algebird-core_2.9.2" % "0.1.11",

      "org.apache.hbase" % "hbase" % HBASE_VERSION excludeAll(excludeNetty, excludeAsm),

      "org.apache.cassandra" % "cassandra-all" % "1.2.5"
        exclude("com.google.guava", "guava")
        exclude("com.googlecode.concurrentlinkedhashmap", "concurrentlinkedhashmap-lru")
        exclude("com.ning","compress-lzf")
        exclude("io.netty", "netty")
        exclude("jline","jline")
        exclude("log4j","log4j")
        exclude("org.apache.cassandra.deps", "avro")
        excludeAll(excludeSnappy)
        excludeAll(excludeCglib)
    )
  ) ++ assemblySettings ++ extraAssemblySettings

  def toolsSettings = sharedSettings ++ Seq(
    name := "spark-tools"
  )

  def graphSettings = sharedSettings ++ Seq(
    name := "spark-graphx"
  )

  def bagelSettings = sharedSettings ++ Seq(
    name := "spark-bagel"
  )

  def mllibSettings = sharedSettings ++ Seq(
    name := "spark-mllib",
    libraryDependencies ++= Seq(
      "org.jblas" % "jblas" % "1.2.3"
    )
  )

  def streamingSettings = sharedSettings ++ Seq(
    name := "spark-streaming",
    resolvers ++= Seq(
      "Akka Repository" at "http://repo.akka.io/releases/"
    ),
    libraryDependencies ++= Seq(
      "org.apache.flume" % "flume-ng-sdk" % "1.2.0" % "compile" excludeAll(excludeNetty, excludeSnappy),
      "com.github.sgroschupf" % "zkclient" % "0.1" excludeAll(excludeNetty),
      "org.twitter4j" % "twitter4j-stream" % "3.0.3" excludeAll(excludeNetty),
      "com.typesafe.akka" % "akka-zeromq" % "2.0.5" excludeAll(excludeNetty)
    )
  )

  def yarnSettings = sharedSettings ++ Seq(
    name := "spark-yarn"
  ) ++ extraYarnSettings

  // Conditionally include the YARN dependencies because some tools look at all sub-projects and will complain
  // if we refer to nonexistent dependencies (e.g. hadoop-yarn-api from a Hadoop version without YARN).
  def extraYarnSettings = if(isYarnEnabled) yarnEnabledSettings else Seq()

  def yarnEnabledSettings = Seq(
    libraryDependencies ++= Seq(
      // Exclude rule required for all ?
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm, excludeCglib),
      "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm, excludeCglib),
      "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm, excludeCglib),
      "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm, excludeCglib)
    )
  )

  def assemblyProjSettings = sharedSettings ++ Seq(
    name := "spark-assembly",
    jarName in assembly <<= version map { v => "spark-assembly-" + v + "-hadoop" + hadoopVersion + ".jar" }
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(
    test in assembly := {},
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.discard
      case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
