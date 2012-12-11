package com.socrata.soql.adapter

import com.socrata.soql.typed.{TypedFF, OrderBy}
import com.socrata.soql.types.SoQLType
import com.socrata.collection.OrderedMap
import com.socrata.soql.names.ColumnName
import util.parsing.input.Position
import com.socrata.soql.SoQLAnalyzer

class SoQLAdapterException(val message: String, val position: Position) extends Exception(message + "\n" + position.longString)

class NotImplementedException(m: String, p: Position) extends SoQLAdapterException(m, p)

trait SoqlAdapter[T] {

  def full(soql: String): Tuple2[T, SoQLAnalyzer[SoQLType]#Analysis]

  def select(selection : OrderedMap[ColumnName, TypedFF[SoQLType]]): T

  def where(filter: TypedFF[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef]): T

  def orderBy(orderBys: Seq[OrderBy[SoQLType]]): T

  def groupBy(groupBys: Seq[TypedFF[SoQLType]], cols: OrderedMap[ColumnName, TypedFF[SoQLType]]): T

  def offset(offset: Option[BigInt]): T

  def limit(limit: Option[BigInt]): T
}

object XlateCtx extends Enumeration {
  type XlateCtx = Value
  val ESLang = Value("es-lang")
}
