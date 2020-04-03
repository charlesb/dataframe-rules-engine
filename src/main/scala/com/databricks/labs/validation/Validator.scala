package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.Result
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions.{
  col, sum, when, lit, struct,
  array, explode, collect_set, collect_list, expr
}
import org.apache.spark.sql.types._
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
  private val byCols = ruleSet.getGroupBys map col

  case class Selects(select: Column*)

  private def buildNulls(columsUsed: Seq[String]): Seq[Column] = {
    val allNulls = Map[String, Column](
      "Failed" -> lit(null).cast(BooleanType).alias("Failed"),
      "Failed_Count" -> lit(null).cast(LongType).alias("Failed_Count")
    )
    allNulls.keys.toSeq.diff(columsUsed).map(k => allNulls(k))
  }

  // Add count of invalids by rule {rule.alias}_cnt
  private def buildBaseSelects(rules: Array[Rule]): Array[Selects] = {
    // Build base selects
    val w = Window.partitionBy(ruleSet.getGroupBys map col: _*)
    rules.map(rule => {
      val isAgg = rule.inputColumn.expr.prettyName == "aggregateexpression"
      val isGrouped = ruleSet.getGroupBys.nonEmpty

      // TODO - Try creating rules as dataframe
      // try using grouping and unioning vs windowing
      // FINAL OUTPUT is mixing the data
      rule.ruleType match {
        case "bounds" =>
          val invalid = rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper
          // first
          val first = if (isAgg && !isGrouped) {
            rule.inputColumn.alias(s"${rule.ruleName}_agg_value")
          } else if (isAgg && isGrouped) {
            rule.inputColumn.over(w).alias(s"${rule.ruleName}_agg_value")
          } else if (!isAgg && isGrouped) {
            sum(when(invalid, 1)
              .otherwise(0)).over(w).alias(s"${rule.ruleName}_count")
          } else {
            sum(when(invalid, 1)
              .otherwise(0)).alias(s"${rule.ruleName}_count")
          }
          // second
          val second = if (isAgg) {
            struct(Seq(
              lit(rule.ruleName).alias("RuleName"),
              lit(rule.ruleType).alias("RuleType"),
              when(col(s"${rule.ruleName}_agg_value") < rule.boundaries.lower ||
                col(s"${rule.ruleName}_agg_value") > rule.boundaries.upper, true)
                .otherwise(false).alias("Failed")) ++ buildNulls(Seq("Failed")): _*)
              .alias(rule.ruleName)
          } else {
            struct(Seq(
              lit(rule.ruleName).alias("RuleName"),
              lit(rule.ruleType).alias("RuleType"),
              col(s"${rule.ruleName}_count").cast(LongType).alias("Failed_Count")) ++
              buildNulls(Seq("Failed_Count")): _*)
              .alias(rule.ruleName)
          }
          Selects(first, second)
        case "validNumerics" => //validNumerics
          // first
          val first = if (!isGrouped) {
            rule.calculatedColumn.alias(s"${rule.ruleName}_agg_value")
          } else {
            rule.calculatedColumn.over(w).alias(s"${rule.ruleName}_agg_value")
          }
          // second
          val second = expr(s"array_except(${rule.ruleName}_agg_value," +
              s"array(${rule.validNumerics.valid.mkString("D,")}D))").alias(s"Invalid_${rule.canonicalColName}s")
          val third = struct(
            lit(rule.ruleType).alias("RuleType"),
            col(s"Invalid_${rule.canonicalColName}s").alias(s"Invalid_${rule.inputColumnName}s"),
            expr(s"size(Invalid_${rule.canonicalColName}s) > 0").alias(s"${rule.ruleName}_Failed"),
            expr(s"size(Invalid_${rule.canonicalColName}s)")
              .cast(LongType).alias(s"${rule.ruleName}_Failed_Count")
          ).alias(rule.ruleName)
          Selects(first, second, third)
      }
    })
  }

  private def genSummaryDF(detailDF: DataFrame): DataFrame = {

    val summaryCols = Array("RuleName", "RuleType", "Failed", "Failed_Count")
    val dfCs = detailDF.columns.diff(ruleSet.getGroupBys)

    val baseSummary = summaryCols.foldLeft(ArrayBuffer[Column]()) {
      case (outs, summaryCol) =>
        val colsByOut = dfCs.map(dfc => {
          val bounds = col(s"$dfc.RuleType") === "bounds"
          summaryCol match {
            case "RuleName" => lit(dfc).alias("Rule")
            case "RuleType" => col(s"$dfc.RuleType")
            case "Failed" => when(bounds, col(s"$dfc.Failed")).alias("Failed")
            case "Failed_Count" => when(bounds, col(s"$dfc.Failed_Count")).alias("Failed_Count")
          }
        })
        outs += array(colsByOut: _*).alias(summaryCol)
    }.toArray

    summaryCols.foldLeft(detailDF.select(byCols ++ baseSummary: _*)) {
      case (df, c) =>
        df.withColumn(c, explode(col(c)))
    }

  }

  private def boundaries: DataFrame = {
    case class ColDetail(colName: String, c: Column)
    val selects = buildBaseSelects(boundaryRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.select.head): _*)
        .select(selects.map(_.select(1)): _*)
    } else {
      ruleSet.getDf
        .select(byCols ++ selects.map(_.select.head): _*)
        .select(byCols ++ selects.map(_.select(1)): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }

    genSummaryDF(detailDF)
      .distinct

  }

  private def categoricals: DataFrame = {
    numericRules.foreach(rule => rule.setCalculatedColumn(collect_set(rule.inputColumn.cast("double"))))
    val selects = buildBaseSelects(numericRules)
    val detailDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.select.head): _*)
        .select(selects.map(_.select(1)): _*)
        .select(selects.map(_.select(2)): _*)
    } else {
      ruleSet.getDf
        .select(byCols ++ selects.map(_.select.head): _*)
        .select(byCols ++ selects.map(_.select(1)): _*)
        .select(byCols ++ selects.map(_.select(2)): _*)
        .dropDuplicates(ruleSet.getGroupBys)
    }
    //
    //    val summaryStructs = getSummaryStructs(detailDF)
    //    val summaryDF = detailDF
    //      .withColumn("kvs", array(summaryStructs: _*))
    //      .select(ruleSet.getGroupBys.map(col) :+ explode(col("kvs")).alias("_kvs"): _*)
    //      .select(ruleSet.getGroupBys.map(col) ++
    //        Seq(col("_kvs.Rule")) ++
    //        summaryOutputsByType.values.map(colName => col(s"_kvs.$colName")): _*)
    //      .withColumn("Validation_Failed",
    //        when('Failed_Count > 0, true)
    //        .when('Failed_Count === 0, false).otherwise('Validation_Failed))

    //    val uniqueSets = numericRules
    //      .map(rule => {
    //        collect_set(rule.inputColumn.cast("double")).alias(s"${rule.canonicalColName}s")
    //      })

    //    val dfWSets = ruleSet.getDf.groupBy(ruleSet.getGroupBys map col: _*)
    //      .agg(uniqueSets.head, uniqueSets.tail: _*)
    //
    //    val detailDF = numericRules.foldLeft(dfWSets) {
    //      case (df, rule) =>
    //        df.withColumn("Rule", lit(rule.ruleName))
    //          .withColumn(s"invalid_${rule.inputColumnName}s",
    //                expr(s"array_except(${rule.canonicalColName}s," +
    //                  s"array(${rule.validNumerics.valid.mkString("D,")}D))"))
    //    }
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
    detailDF //.select(col("Valid_Stores.Invalid_store_ids"))
  }

  private def validateStringRules: Unit = ???

  private def validatedateTimeRules: Unit = ???

  private def validateComplexRules: Unit = ???

  private def unifySummary: Unit = ???

  private[validation] def validate: DataFrame = {

    //    if (boundaryRules.nonEmpty) validateBoundaryRules()
    //    if (.nonEmpty)

    if (ruleSet.getRules.exists(_.ruleType == "bounds")) boundaries
    //    if (ruleSet.getRules.exists(_.ruleType == "validNumerics")) validateNumericRules
    else sc.parallelize(Seq((1, 2, 3))).toDF
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}