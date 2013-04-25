package com.socrata.es.soql.query

import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.{OrderBy, CoreExpr}

trait SoqlAdapter[T] {

  def full(soql: String): Tuple2[T, SoQLAnalysis[SoQLAnalysisType]]

  def select(selection : OrderedMap[ColumnName, CoreExpr[SoQLAnalysisType]]): T

  def where(filter: CoreExpr[SoQLAnalysisType], xlateCtx: Map[XlateCtx.Value, AnyRef]): T

  def orderBy(orderBys: Seq[OrderBy[SoQLAnalysisType]]): T

  def groupBy(groupBys: Seq[CoreExpr[SoQLAnalysisType]], cols: OrderedMap[ColumnName, CoreExpr[SoQLAnalysisType]]): T

  def offset(offset: Option[BigInt]): T

  def limit(limit: Option[BigInt]): T
}

object XlateCtx extends Enumeration {
  type XlateCtx = Value
  val ESLang = Value("es-lang")
  val LowercaseStringLiteral = Value("lower-str-lit")
  val TryCatch = Value("try-catch")
}
