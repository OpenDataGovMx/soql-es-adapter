package com.socrata.es.facet

import scala.language.existentials
import com.rojoma.json.ast.{JObject, JValue}
import com.socrata.soql.typed.{FunctionCall, CoreExpr, ColumnRef}
import com.socrata.soql.types.{SoQLAnalysisType, SoQLText, SoQLNumber}
import com.socrata.soql.environment.ColumnName

sealed trait FacetType {
  val name: String
}

object FacetType {
  def apply(s: String): FacetType = map(s)

  private val map = Map(
    TermsStatsFacetType.name -> TermsStatsFacetType,
    TermsFacetType.name -> TermsFacetType,
    StatisticalFacetType.name -> StatisticalFacetType,
    ColumnsFacetType.name -> ColumnsFacetType)
}

case object TermsStatsFacetType extends FacetType { val name = "ts"}

case object TermsFacetType extends FacetType { val name = "t"}

case object StatisticalFacetType extends FacetType {
  val name = "s"
  val groupKey = "_"
}

case object ColumnsFacetType extends FacetType {
  val name = "c"
  val groupKey = "_multi"
}


trait Facet {

  def toJObject: JObject = toJValue.asInstanceOf[JObject]

  protected def toJValue: JValue
}


object Facet {

  def apply(colName: ColumnName, expr: CoreExpr[SoQLAnalysisType]): Facet = {

    expr match {
      case FunctionCall(function, Seq()) if function.isAggregate =>
        StatisticalFacet(None)
      case FunctionCall(function, Seq(cr@ColumnRef(aggColName, SoQLNumber))) if function.isAggregate =>
        StatisticalFacet(Some(cr))
      case FunctionCall(function, Seq(cr@ColumnRef(aggColName, SoQLText))) if function.isAggregate =>
        TermsFacet(cr, function.name)
      case unk =>
        throw new Exception("unsupported")
    }
  }
}
