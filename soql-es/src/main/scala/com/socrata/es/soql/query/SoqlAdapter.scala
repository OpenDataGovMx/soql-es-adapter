package com.socrata.es.soql.query

import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.{SoQLType, SoQLAnalysisType}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.{OrderBy, CoreExpr}

trait SoqlAdapter[T] {

  def full(soql: String): Tuple2[T, SoQLAnalysis[SoQLType]]

  def full(analysis: SoQLAnalysis[SoQLType]): T

  def select(selection : OrderedMap[ColumnName, CoreExpr[SoQLType]]): T

  def where(filter: CoreExpr[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef]): T

  def orderBy(orderBys: Seq[OrderBy[SoQLType]]): T

  def groupBy(groupBys: Seq[CoreExpr[SoQLType]], cols: OrderedMap[ColumnName, CoreExpr[SoQLType]]): T

  def offset(offset: Option[BigInt]): T

  def limit(limit: Option[BigInt]): T
}

object XlateCtx extends Enumeration {
  type XlateCtx = Value
  val ESLang = Value("es-lang")
  val LowercaseStringLiteral = Value("lower-str-lit")
  val TryCatch = Value("try-catch")
}
