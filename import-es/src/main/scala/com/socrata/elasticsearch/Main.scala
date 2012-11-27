package com.socrata.elasticsearch

import org.apache.commons.cli.Options
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.PosixParser
import com.socrata.injest.ESImport
import com.socrata.rows.ESHttpGateway
import com.socrata.soql.adapter.elasticsearch.ESQuery
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.adapter.SoQLAdapterException

object Main extends App {

  private val options: Options = new Options()
  options.addOption("es", true, "elastic search service endpoint")
  options.addOption("r", true, "resource_name")
  options.addOption("f", true, "full csv file name")
  options.addOption("bs", true, "batch size (default 1000)")
  options.addOption("m", false, "add column map")

  private val cmdParser: CommandLineParser = new PosixParser()
  private val cmd: org.apache.commons.cli.CommandLine = cmdParser.parse(options, args)

  val fileName = Option(cmd.getOptionValue("f"))
  val resource = Option(cmd.getOptionValue("r"))
  val batchSize = Option(cmd.getOptionValue("bs")).getOrElse("1000").toInt
  val es = Option(cmd.getOptionValue("es")).getOrElse("http://localhost:9200")

  if (resource.isEmpty) {
    val helpMsg = options.getOptions.toArray.map { o =>
      val opt = o.asInstanceOf[org.apache.commons.cli.Option]
      "-%s %s".format(opt.getOpt(), opt.getDescription) }
      .mkString("\n")
    println(helpMsg)
  } else {
    if (fileName.isDefined) {
      println("importing %s directly into %s".format(fileName.get, resource.get))
      ESImport.go(resource.get, fileName.get, batchSize, cmd.hasOption("m"), es)
      println("done importing")
    } else {
      esSoqlPrompt()
    }
  }

  sys.exit()

  private def esSoqlPrompt() {
    val esGateway = new ESHttpGateway(resource.get, esBaseUrl = es)
    val esQuery = new ESQuery(resource.get, esGateway)

    while(true) {
      val cmd = readLine("soql %s>".format(resource.get))
      if(cmd == null || cmd.toLowerCase == "exit" || cmd.toLowerCase == "quit")
        return
      try {
        val qryStr = esQuery.full(cmd)
        println("Elastic Search Query String:\n" + qryStr)
        val result = esGateway.search(qryStr)
        println("Result:\n" + result)
      } catch {
        case e: SoQLException => println(e.getMessage)
        case e: SoQLAdapterException => println(e.getMessage)
        case e: java.net.ConnectException =>
          println(e.getMessage + "\nMake sure that elasticsearch server is running.")
      }
    }
  }
}
