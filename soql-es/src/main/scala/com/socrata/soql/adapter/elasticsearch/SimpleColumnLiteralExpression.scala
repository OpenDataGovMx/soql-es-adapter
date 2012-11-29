package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed.{TypedLiteral, ColumnRef, TypedFF}

/** *
  * Many times, stores can only handle expressions where lhs is column and rhs is literal.
  * If the child expressions are compound or column to column, a different coarse of action is needed.
  * This class helps recognize such simple usage pattern.
  */
class SimpleColumnLiteralExpression[T](val col: TypedFF[T], val lit: TypedFF[T], val lit2: Option[TypedFF[T]], val lhsIsColumn: Boolean)

object SimpleColumnLiteralExpression {

  def unapply[T](exprs: Seq[TypedFF[T]]): Option[SimpleColumnLiteralExpression[T]] = {

    val colsLitsCpounds = partitionExprs[T](exprs)
    if (isSimpleColumnLiteral(colsLitsCpounds)) {
      val lit2 = colsLitsCpounds._2 match {
        case h :: h2 :: _ => Some(h2)
        case _ => None
      }
      Some(new SimpleColumnLiteralExpression[T](
        colsLitsCpounds._1.head, colsLitsCpounds._2.head, lit2, exprs.head.isInstanceOf[ColumnRef[_]]))
    } else {
      None
    }
  }

  private def isSimpleColumnLiteral[T](exprs: Tuple3[Seq[TypedFF[T]], Seq[TypedFF[T]], Seq[TypedFF[T]]]) = {
    exprs._1.size == 1 && exprs._2.size >= 1 && exprs._3.isEmpty
  }

  /**
   * Partition expression into 3 types - columns, literal and compounds.
   */
  private def partitionExprs[T](exprs: Seq[TypedFF[T]]) = {
    val colsAndNonCols = exprs.partition(_.isInstanceOf[ColumnRef[_]])
    val litsAndNonLits = colsAndNonCols._2.partition(_.isInstanceOf[TypedLiteral[_]])
    val cols = colsAndNonCols._1
    val lits = litsAndNonLits._1
    val cpounds = litsAndNonLits._2
    Tuple3(cols, lits, cpounds)
  }
}
