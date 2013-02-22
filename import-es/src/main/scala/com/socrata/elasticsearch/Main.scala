package com.socrata.elasticsearch

import org.apache.commons.cli.Options
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.PosixParser
import com.rojoma.simplearm.util._
import com.socrata.injest.ESImport
import com.socrata.rows.{GatewayException, ESHttpGateway}
import com.socrata.soql.adapter.elasticsearch.{ESResultSet, ESQuery}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.adapter.SoQLAdapterException
import java.io.InputStream
import com.socrata.es.meta.{ESType, ESIndex}

object Main extends App {

  private val options: Options = new Options()
  options.addOption("es", true, "elastic search service endpoint")
  options.addOption("r", true, "resource_name")
  options.addOption("f", true, "full csv file name")
  options.addOption("bs", true, "batch size (default 1000)")
  options.addOption("m", true, "add column map")

  private val cmdParser: CommandLineParser = new PosixParser()
  private val cmd: org.apache.commons.cli.CommandLine = cmdParser.parse(options, args)

  val fileName = Option(cmd.getOptionValue("f"))
  var resource = Option(cmd.getOptionValue("r"))
  val batchSize = Option(cmd.getOptionValue("bs")).getOrElse("1000").toInt
  val es = Option(cmd.getOptionValue("es")).getOrElse("http://localhost:9200")
  val mapFile = fileName match {
    case Some(fName) => Some(Option(cmd.getOptionValue("m")).getOrElse(fName.replace(".csv", "-type.csv").replace(".gz", "")))
    case None => None
  }

  if (resource.isEmpty) {
    val helpMsg = options.getOptions.toArray.map { o =>
      val opt = o.asInstanceOf[org.apache.commons.cli.Option]
      "-%s %s".format(opt.getOpt(), opt.getDescription) }
      .mkString("\n")
    println(helpMsg)
  } else {
    if (fileName.isDefined) {
      println("importing %s directly into %s".format(fileName.get, resource.get))
      ESImport.go(resource.get, fileName.get, mapFile.get, batchSize, cmd.hasOption("m"), es)
      println("done importing")
    } else {
      esSoqlPrompt()
    }
  }

  sys.exit()

  private def esSoqlPrompt() {
    var limit: Option[BigInt] = None
    var esGateway = new ESHttpGateway(ESIndex(resource.get), ESType("data"), esBaseUrl = es)
    var esQuery = new ESQuery(resource.get, esGateway, limit)

    // switch datasetName version
    val switchDatasetCmd = """switch\s+(.+)\s+(.+)""".r
    val limitCmd = """limit\s+(.+)""".r

    while(true) {
      val cmd = readLine("soql %s>".format(resource.get))
      try {
        cmd match {
          case "" => println("%s on %s".format(resource.get, es))
          case "exit" | "quit" | "e" | "q" => return
          case "map" =>
            val ctx = esGateway.getDataContext()
            for( ((colName, soqlType), idx) <- ctx.schema.zipWithIndex) {
              println("%d: %s: %s".format(idx + 1, colName.name, soqlType.name.name))
            }
          case switchDatasetCmd(name, typ) =>
            resource = Some(name)
            esGateway = new ESHttpGateway(ESIndex(resource.get), ESType(typ), esBaseUrl = es)
            esQuery = new ESQuery(resource.get, esGateway, limit)
          case limitCmd(lim) =>
            limit = Some(BigInt(lim))
            esQuery = new ESQuery(resource.get, esGateway, limit)
          case _ =>
              val (qry, analysis) = esQuery.full(cmd)
              println("\nElastic Search Query String:\n" + qry)
              using(esGateway.search(qry)) { inputStream: InputStream =>
                val (total, rowStream) = ESResultSet.parser(analysis.isGrouped, inputStream).rowStream()
                println("\nResult total rows: %d, returned rows: %d".format(total.getOrElse(-1), rowStream.size))
                println(rowStream.mkString("[", ",\n", "]"))
              }
        }
      } catch {
        case e: SoQLException => println(e.getMessage)
        case e: SoQLAdapterException => println(e.getMessage)
        case e: GatewayException => println(e.getMessage)
        case e: java.net.ConnectException =>
          println(e.getMessage + "\nMake sure that elasticsearch server is running.")
      }
    }
  }
}
