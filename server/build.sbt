enablePlugins(JavaAppPackaging)

enablePlugins(DockerPlugin)

maintainer in Docker := "Alexey Ponkin <alexey.ponkin@gmail.com>"

packageSummary in Docker := "Server for probalistic data structures"

dockerExposedPorts ++= Seq(22022)

dockerExposedVolumes ++= Seq("data")

daemonUser := "blooms"

mappings in Universal += {
  // we are using the reference.conf as default application.conf
  // the user can override settings here
  val conf = (resourceDirectory in Compile).value / "reference.conf"
  conf -> "conf/application.conf"
}

mappings in Universal += {
  val logging = (resourceDirectory in Compile).value / "logging.properties"
  logging -> "conf/logging.properties"
}

javaOptions in Universal ++= Seq(
    // -J params will be added as jvm parameters
    "-J-Xmx2g",
    "-J-Xms2g"
)

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf""""
bashScriptExtraDefines += """addJava "-Djava.util.logging.config.file=${app_home}/../conf/logging.properties""""
