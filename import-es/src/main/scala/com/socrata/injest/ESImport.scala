package com.socrata.injest

import au.com.bytecode.opencsv.CSVReader
import java.io._
import com.rojoma.simplearm.util._
import com.socrata.rows.{ESColumnMap, ESHttpGateway}
import com.socrata.util.strings.CamelCase
import java.util.zip.GZIPInputStream
import com.rojoma.json.ast.JNull
import com.socrata.es.meta.ESIndex

object ESImport {

  def go(resource: String, fileName: String, mapFile: String, batchSize: Int, addColumnMap: Boolean, es: String) {

    val file = new File(fileName)
    var is : InputStream = new FileInputStream(file)
    if (fileName.toLowerCase.endsWith(".gz")) {
      is = new GZIPInputStream(is)
    }
    val fileReader = new InputStreamReader(is)

    val esGateway = new ESHttpGateway(ESIndex(resource), batchSize = batchSize, esBaseUrl = es)

    using (new CSVReader(fileReader)) { csv =>
      val headerName: Array[String] = csv.readNext().map(f =>
        // Essentially underscorize column name.
        "[^\\d\\w]".r.replaceAllIn(CamelCase.decamelize(CamelCase.camelize(f.toLowerCase, false)), "_") )
      val headerWithIndex = headerName.zipWithIndex.toMap
      val esColumnMap: Array[ESColumnMap] = columnTypes(fileName, mapFile, headerName)

      esGateway.ensureIndex()
      if (addColumnMap) {
        esGateway.updateEsColumnMapping(headerName.zip(esColumnMap).toMap)
      }

      var rawData: Array[String] = csv.readNext()
      while (Option(rawData).isDefined) {
        val nonEmptyData = headerWithIndex.collect {
          case (fieldName, position) if !rawData(position).isEmpty =>
            (fieldName -> esColumnMap(position).toES(rawData(position)))
        }
        // get rid of nulls
        val data = nonEmptyData.filter{ case (k, v) => v != JNull }
        esGateway.addRow(data)
        rawData = csv.readNext()
      }
      esGateway.flush()
    }
  }

  /**
   * Get column types either from a *-type.csv file
   * or assume everything is text if the type file is not found.
   * @param fileName - csv file name (without type)
   * @param header - header column array used in case if there is no type file.
   * @return - array of column map.
   */
  private def columnTypes(fileName: String, mapFile: String, header: Array[String]): Array[ESColumnMap] = {

    import com.socrata.rows.ESColumnMap._

    val file = new File(mapFile)
    if (file.exists()) {
      val fileReader = new FileReader(file)
      using (new CSVReader(fileReader)) { csv =>
        val headerType = csv.readNext()
        headerType.map(ESColumnMap(_))
      }
    } else {
      header.map(x => ESColumnMap("text"))
    }
  }
}

