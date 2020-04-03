package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.Result
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions.{
  col, sum, when, lit, struct,
  array, explode, collect_set, expr
}
import utils.Helpers._

import scala.collection.mutable.ArrayBuffer

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  import spark.implicits._

  private val boundaryRules = ruleSet.getRules.filter(_.ruleType == "bounds")
  private val numericRules = ruleSet.getRules.filter(_.ruleType == "validNumerics")
  private val stringRules = ruleSet.getRules.filter(_.ruleType == "validStrings")
  private val dateTimeRules = ruleSet.getRules.filter(_.ruleType == "dateTime")
  private val complexRules = ruleSet.getRules.filter(_.ruleType == "complex")
  private val rulesReport = ArrayBuffer[Result]()
  private val validationSummaryDF: DataFrame = Seq.empty[Result].toDF

  case class Selects(baseSelects: Column, topSelects: Column)

  //  private def genAggCol(rule: Rule): Column = {
  //    val funcRaw = rule.aggFunc.get.apply(rule.inputColumn).toString()
  //    val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
  //    rule.aggFunc.get(rule.inputColumn).cast("double")
  //      .alias(s"${getColumnName(rule.inputColumn)}_${funcName}")
  //    }

  // Add count of invalids by rule {rule.alias}_cnt
  private def getBoundarySelects(rules: Array[Rule]): Array[Selects] = {
    val w = Window.partitionBy(ruleSet.getGroupBys map col: _*)
    rules.map(rule => {
      val isAgg = rule.inputColumn.expr.prettyName == "aggregateexpression"
      val isGrouped = ruleSet.getGroupBys.nonEmpty

      val baseSelects = if (isAgg && !isGrouped) {
        rule.inputColumn.alias(s"${rule.ruleName}_agg_value")
      } else if (isAgg && isGrouped) {
        rule.inputColumn.over(w).alias(s"${rule.ruleName}_agg_value")
      } else if (!isAgg && isGrouped) {
        sum(when(rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper, 1)
          .otherwise(0)).over(w).alias(s"${rule.ruleName}_count")
      } else {
        sum(when(rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper, 1)
          .otherwise(0)).alias(s"${rule.ruleName}_count")
      }

      val topSelects = if (isAgg) {
        when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
          col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
          .otherwise(false).alias(s"${rule.ruleName}")
      } else {
        col(s"${rule.ruleName}_count").alias(rule.ruleName)
      }
      Selects(baseSelects, topSelects)
    })
  }

  private val summaryOutputsByType = Map(
    "BooleanType" -> "Validation_Failed",
    "LongType" -> "Failed_Count"
  )

  private def getSummaryStructs(detailDF: DataFrame): Array[Column] = {
    val colsByType = detailDF.dtypes.filter { case (c, _) => !ruleSet.getGroupBys.contains(c) }
      .groupBy(_._2).map(typeMap => typeMap._1 -> typeMap._2.map(_._1))

    val castByType = Map(
      "BooleanType" -> "boolean",
      "IntegerType" -> "int",
      "LongType" -> "long"
    )

    colsByType.flatMap(tc => {
      val colType = tc._1
      val colsForType = tc._2
      colsForType.map(c => { // col of type
        val outputCols = summaryOutputsByType.map(output => { // for output type
          if (colType == output._1) { // if col matches output type
            col(c).alias(output._2) // create col if type is same
          } else {
            // TODO -- colType is wrong
            lit(null).cast(castByType(output._1)).alias(output._2) // Create null if diff type
          }
        }).toArray
        struct(
          lit(c).alias("Rule") +: outputCols: _*
        )
      })
    }).toArray
  }

  private def boundsSummaryDF: DataFrame = {
    case class ColDetail(colName: String, c: Column)
    val selects = getBoundarySelects(boundaryRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.baseSelects): _*)
//        .select(selects.map(_.topSelects): _*)
    } else {
      val byCols = ruleSet.getGroupBys map col
      ruleSet.getDf
        .select(byCols ++ selects.map(_.baseSelects): _*)
//        .select(byCols ++ selects.map(_.topSelects): _*)
//        .dropDuplicates(ruleSet.getGroupBys)
    }

    val metaDF = sc.parallelize(boundaryRules.map(rule =>
      (rule.ruleName,
        rule.ruleType,
        Array(rule.boundaries.lower, rule.boundaries.upper)
      )
    )).toDF("Rule", "Rule_Type", "Valid")

    val summaryStructs = getSummaryStructs(detailDF)

    // Can convert to map if more filters necessary
    val detailFilter = 'Validation_Failed === true || 'Failed_Count > 0
    val summaryDF = detailDF
      .withColumn("kvs", array(summaryStructs: _*))
      .select(ruleSet.getGroupBys.map(col) :+ explode(col("kvs")).alias("_kvs"): _*)
      .select(ruleSet.getGroupBys.map(col) ++
        Seq(col("_kvs.Rule")) ++
        summaryOutputsByType.values.map(colName => col(s"_kvs.$colName")): _*)
    val filteredSummaryDF = if (detailLvl <= 1) summaryDF.filter(detailFilter) else summaryDF
    detailDF
//    val orderBys = if (ruleSet.getGroupBys.nonEmpty) s"Rule, ${ruleSet.getGroupBys.mkString(",")}" else "Rule"
//
//    filteredSummaryDF
//      .join(metaDF, "Rule") // Add UUID to each rule and join on that
//      .orderBy(orderBys)
  }

  //  // SELECT array_except(array(1, 2, 3), array(1, 3, 5));
  //  val ar = Array(1001, 1002).mkString(",")
  //  val x = Seq.empty[Column]
  //  display(
  //    df
  //      .groupBy(x: _*)
  //      .agg(collect_set('store_id).alias("store_ids"))
  //      .withColumn("invalid_store_ids", expr(s"array_except(store_ids, array(${ar}))"))
  //  )

  private def validateNumericRules: DataFrame = {
    val numericRules = ruleSet.getRules.filter(rule => rule.ruleType == "validNumerics")
    val uniqueSets = numericRules
      .map(rule => {
        collect_set(rule.inputColumn.cast("double")).alias(s"${rule.canonicalColName}s")
      })

    val dfWSets = ruleSet.getDf.groupBy(ruleSet.getGroupBys map col: _*)
      .agg(uniqueSets.head, uniqueSets.tail: _*)

    val detailDF = numericRules.foldLeft(dfWSets) {
      case (df, rule) =>
        df.withColumn("Rule", lit(rule.ruleName))
          .withColumn(s"invalid_${rule.inputColumnName}s",
            expr(s"array_except(${rule.canonicalColName}s," +
              s"array(${rule.validNumerics.valid.mkString("D,")}D))"))
    }
//    val metaDF = sc.parallelize(boundaryRules.map(rule =>
//      (rule.ruleName,
//        rule.ruleType,
//        Array(rule.boundaries.lower, rule.boundaries.upper)
//      )
//    )).toDF("Rule", "Rule_Type", "Valid")
//
//    detailDF.withColumn("Rule", lit(rule.ruleName))
//      .withColumn("Validation_Failed",
//        expr(s"size(invalid_${rule.inputColumnName}s) > 0"))
//      .withColumn("Failed_Count", expr(s"size(invalid_${rule.inputColumnName}s)"))
//      .withColumn("Rule_Type", lit(rule.ruleType))
//      .withColumn("Valid", lit(rule.validNumerics.valid))
//
    detailDF
  }

  private def validateStringRules: Unit = ???

  private def validatedateTimeRules: Unit = ???

  private def validateComplexRules: Unit = ???

  private def unifySummary: Unit = ???

  private[validation] def validate: DataFrame = {

    //    if (boundaryRules.nonEmpty) validateBoundaryRules()
    //    if (.nonEmpty)

    if (ruleSet.getRules.exists(_.ruleType == "bounds"))boundsSummaryDF
//    if (ruleSet.getRules.exists(_.ruleType == "validNumerics")) validateNumericRules
    else sc.parallelize(Seq((1, 2, 3))).toDF
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}