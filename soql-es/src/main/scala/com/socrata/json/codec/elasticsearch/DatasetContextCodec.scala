package com.socrata.json.codec.elasticsearch

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{POption, PObject, Variable}
import com.rojoma.json.ast.{JObject, JValue}
import com.socrata.soql.names.{ColumnName, TypeName}
import com.socrata.soql.{SchemalessDatasetContext, DatasetContext}
import com.socrata.soql.types.SoQLType

import org.apache.commons.lang.NotImplementedException

class DatasetContextCodec(resource: Option[String] = None) extends JsonCodec[DatasetContext[SoQLType]] {
  import DatasetContextCodec._

  implicit val schemalessDatasetContext = new SchemalessDatasetContext {
    val locale = com.ibm.icu.util.ULocale.ENGLISH
  }

  private val typeVar = Variable[String]()
  private val formatVar = Variable[String]()
  private val fieldPat = PObject(
    "type" -> typeVar,
    "format" -> POption(formatVar)
  )

  private def decodeType(v: JValue) = {
    v match {
      case fieldPat(result) =>
        val soqlTypeName = es2SoqlType(typeVar(result))
        val soqlType = SoQLType.typesByName(TypeName(soqlTypeName))
        soqlType
    }
  }

  private def skipToValue(jvalue: Option[JValue], path: Seq[String]): Option[JValue] = {
    path match {
      case Nil => jvalue
      case h :: t =>
        val jobject = jvalue match {
          case Some(jo: JObject) => jo.get(h)
          case _ => None
        }
        skipToValue(jobject, t)
    }
  }

  def encode(myObj: DatasetContext[SoQLType]): JValue = {
    throw new NotImplementedException
  }

  def decode(jValue: JValue): Option[DatasetContext[SoQLType]] = {

    val optPath = if (resource.isDefined) Seq(resource.get) else Nil

    skipToValue(Some(jValue), optPath ++ Seq("data", "properties")) match {
      case Some(jo : JObject) =>
        val datasetContext = new DatasetContext[SoQLType] {
          implicit val ctx = this
          val locale = com.ibm.icu.util.ULocale.ENGLISH
          val colMap = jo.foldLeft(com.socrata.collection.OrderedMap.empty[ColumnName, SoQLType]){(ordMap, kv) =>
            ordMap + (ColumnName(kv._1) -> decodeType(kv._2))
          }
          val schema = colMap
        }

        Some(datasetContext)
      case _ => None
    }
  }
}

object DatasetContextCodec {
  val es2SoqlType = Map(
    "string" -> "text",
    "boolean" -> "boolean",
    "geo_point" -> "location",
    "date" -> "floating_timestamp",
    "double" -> "number"
  )
}