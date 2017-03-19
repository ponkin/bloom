resolvers += Resolver.typesafeRepo("releases")
resolvers += "twitter-repo" at "https://maven.twttr.com"

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.4")
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.13.0")
