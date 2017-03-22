lazy val commonSettings = Seq(
  organization := "com.github.ponkin",
  version := "0.10",
  scalaVersion := "2.11.8",
  licenses += ("Apache-2.0", url("https://choosealicense.com/licenses/apache-2.0/")),
  scalacOptions ++= Seq (
    "-unchecked",
    "-deprecation",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:reflectiveCalls",
    "-language:implicitConversions"
  ),
  bintrayRepository := "bloom"
)
  
lazy val driver = project.in(file("driver")).
  settings(commonSettings: _*).
  settings(    
    name := "bloom-driver",
    libraryDependencies ++= Seq (
      "com.twitter" %% "scrooge-core" % "4.13.0",
      "org.apache.thrift" % "libthrift" % "0.9.3",
      "com.twitter" %% "finagle-thrift" % "6.41.0"
    )    
  )

lazy val connector = project.in(file("spark-connector")).
  settings(commonSettings: _*).
  settings(    
    name := "spark-bloom-connector",
    libraryDependencies ++= Seq (
      "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"
    )
  ).dependsOn(driver)

lazy val core = project.in(file("core")).
  settings(commonSettings: _*).
  settings(    
    name := "bloom-core",
    crossPaths       := false,
    autoScalaLibrary := false,
    libraryDependencies ++= Seq (
      "org.scalatest"      %% "scalatest"     % "2.2.1" % Test,
      "org.scalacheck"     %% "scalacheck"    % "1.12.1" % Test,
      "commons-io"         %  "commons-io"    % "2.5" % Test,
      "org.apache.commons" %  "commons-lang3" % "3.5" % Test
    )
  )

lazy val server = project.in(file("server")).
  settings(commonSettings: _*).
  settings(    
    name := "bloom-server",
    crossPaths       := false,
    autoScalaLibrary := false,
    libraryDependencies ++= Seq (
      "com.github.melrief" %% "pureconfig"    % "0.4.0",
      "org.log4s"          %% "log4s"         % "1.3.3",
      "commons-io"         %  "commons-io"    % "2.5",
      "org.apache.commons" %  "commons-lang3" % "3.5",
      "com.google.guava"   %  "guava"         % "20.0",
      "org.slf4j"          %  "slf4j-jdk14"   % "1.7.21",
      "com.twitter"        %% "chill"         % "0.9.1",
      "org.scalatest"      %% "scalatest"     % "2.2.1" % Test,
      "org.scalacheck"     %% "scalacheck"    % "1.12.1" % Test
    )  
  ).dependsOn(driver, core)
