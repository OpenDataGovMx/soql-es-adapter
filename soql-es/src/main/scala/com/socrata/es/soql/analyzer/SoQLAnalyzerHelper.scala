package com.socrata.es.soql.analyzer

import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer, AnalysisDeserializer, AnalysisSerializer}
import com.socrata.soql.functions.{Function, SoQLFunctions, SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.environment.{TypeName, DatasetContext}
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import com.google.protobuf.{CodedInputStream, CodedOutputStream}

object SoQLAnalyzerHelper {

  val serializer = new AnalysisSerializer(serializeAnalysisType)

  val deserializer = new AnalysisDeserializer(deserializeType, functionsByIdentity)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String, datasetCtx: DatasetContext[SoQLType]): SoQLAnalysis[SoQLType] = {
    implicit val ctx: DatasetContext[SoQLAnalysisType] = toAnalysisType(datasetCtx)

    val analysis: SoQLAnalysis[SoQLAnalysisType] = analyzer.analyzeFullQuery(soql)
    val baos = new ByteArrayOutputStream
    serializer(baos, analysis)
    deserializer(new ByteArrayInputStream(baos.toByteArray))
  }

  private def serializeAnalysisType(out: CodedOutputStream, t: SoQLAnalysisType) {
    out.writeStringNoTag(t.name.name)
  }

  private def deserializeType(in: CodedInputStream): SoQLType = {
    SoQLType.typesByName(TypeName(in.readString()))
  }

  private val functionsByIdentity = SoQLFunctions.allFunctions.foldLeft(Map.empty[String, Function[SoQLType]]) { (acc, func) =>
    acc + (func.identity -> func)
  }

  private def toAnalysisType(datasetCtx: DatasetContext[SoQLType]): DatasetContext[SoQLAnalysisType] = {
    val soqlAnalysisTypeSchema = datasetCtx.schema.map { case (columnName, soqlType) =>
      (columnName, soqlType.asInstanceOf[SoQLAnalysisType])
    }
    val analysisContext = new DatasetContext[SoQLAnalysisType] {
      val schema = soqlAnalysisTypeSchema
    }
    analysisContext
  }
}
