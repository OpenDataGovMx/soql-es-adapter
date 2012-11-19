package com.socrata.injest

import au.com.bytecode.opencsv.CSVReader
import java.io.{File, FileReader}
import com.rojoma.simplearm.util._
import com.socrata.rows.{ESColumnMap, ESHttpGateway}
import com.socrata.util.strings.CamelCase

object ESImport {

  def go(resource: String, fileName: String, batchSize: Int, addColumnMap: Boolean, es: String) {

    val file = new File(fileName)
    val fileReader = new FileReader(file)
    val esGateway = new ESHttpGateway(resource, batchSize = batchSize, esBaseUrl = es)

    using (new CSVReader(fileReader)) { csv =>
      val headerName: Array[String] = csv.readNext().map(f => CamelCase.decamelize(CamelCase.camelize(f)))
      val headerWithIndex = headerName.zipWithIndex.toMap
      val esColumnMap: Array[ESColumnMap] = columnTypes(fileName, headerName)

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
        esGateway.addRow(nonEmptyData)
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
  private def columnTypes(fileName: String, header: Array[String]): Array[ESColumnMap] = {

    import com.socrata.rows.ESColumnMap._

    val typeFileName = fileName.replaceAll(".csv", "-type.csv")
    val file = new File(typeFileName)
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

