package com.socrata.soql.adapter

import com.socrata.soql.typed.{CoreExpr, OrderBy}
import com.socrata.soql.types.SoQLType
import util.parsing.input.Position
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName

class SoQLAdapterException(val message: String, val position: Position) extends Exception(message + "\n" + position.longString)

class NotImplementedException(m: String, p: Position) extends SoQLAdapterException(m, p)

trait SoqlAdapter[T] {

  def full(soql: String): Tuple2[T, SoQLAnalyzer[SoQLType]#Analysis]

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
}
