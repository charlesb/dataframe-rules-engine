package com.databricks.labs.validation

import com.databricks.labs.validation.utils.{MinMaxFunc, SparkSessionWrapper}
import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef, RuleDefinition, RuleDefinitions}
import com.databricks.labs.validation.utils.Helpers._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{max, min}

import scala.collection.mutable.ArrayBuffer

class RuleSet extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var _df: DataFrame = _
  private var _isGrouped: Boolean = _
  private var _groupBys: Seq[String] = Seq.empty[String]
  private val _rules = ArrayBuffer[Rule]()

  private def setDF(value: DataFrame): this.type = {
    _df = value
    this
  }

  private def setIsGrouped(value: Boolean): this.type = {
    _isGrouped = value
    this
  }

  private def setGroupByCols(value: Seq[String]): this.type = {
    _groupBys = value
    this
  }

  private[validation] def getDf: DataFrame = _df

  private[validation] def getGroupedFlag: Boolean = _isGrouped

  private[validation] def getGroupBys: Seq[String] = _groupBys

  def getRules: Array[Rule] = _rules.toArray


  def addMinMaxRules(ruleName: String,
                     inputColumn: Column,
                     boundaries: Bounds,
                     by: Column*
                    ): this.type = {
    val rules = Array(
      Rule(s"${ruleName}_min", min(inputColumn), boundaries),
      Rule(s"${ruleName}_max", max(inputColumn), boundaries)
    )
    add(rules)
  }

  def add(rules: Seq[Rule]): this.type = {
    rules.foreach(rule => _rules.append(rule))
    this
  }

  def add(rule: Rule): this.type = {
    _rules.append(rule)
    this
  }

  def add(ruleSet: RuleSet): RuleSet = {
    new RuleSet().setDF(ruleSet.getDf)
      .setIsGrouped(ruleSet.getGroupedFlag)
      .add(ruleSet.getRules)
  }

//  val ruleExample = RuleDefinition("testRule", "bounds", false, Bounds(0.02, 100.0), null)
//  val ruleExample2 = RuleDefinition("testRule", "bounds", false, null, Array(100.2, 100.5, 200.3))
//  val rules = RuleDefinitions(Array(ruleExample, ruleExample2))
//  // val rules2 = RuleDefinitions(ruleExample2)
//  val rulesDF = sc.parallelize(Seq(rules)).toDS.toDF
//
//  val dfWRules = df.crossJoin(rulesDF)

//  def appendRulesToDF(): Unit = {
//    val ruleDefinitions = _rules.map(rule => {
//      RuleDefinition(rule.ruleName, rule.ruleType, rule.isAgg, rule.boundaries, rule.validNumerics)
//    })
//    val ruleDefsDF = sc.parallelize(ruleDefinitions).toDS.toDF
//    _df = getDf.crossJoin(ruleDefsDF)
//  }

  /**
   * Logic to actually test compliance with provided rules added through the builder
   *
   * @return this but is marked private
   */

  // Can't have groupedDataset without Bys
  // Unique rule names
  // What else?
  private def validateRules(): Unit = {
    require(getRules.map(_.ruleName).distinct.length == getRules.map(_.ruleName).length,
      s"Duplicate Rule Names: ${getRules.map(_.ruleName).diff(getRules.map(_.ruleName).distinct).mkString(", ")}")
  }

//  private def appendCanonicalCols(): Unit = {
//    _df = _rules.foldLeft(_df) {
//      case (df, rule) =>
//        val x = rule.canonicalColName
//        val y = rule.inputColumn
//        val z = rule.canonicalCol
//        df.withColumn(rule.canonicalColName, rule.inputColumn)
//    }
//  }

  /**
   * Call the action once all rules have been applied
   *
   * @return Tuple of Dataframe report and final boolean of whether all rules were passed
   */
  def validate(detailLevel: Int = 1): (DataFrame, Boolean) = {
    validateRules()
//    appendCanonicalCols()
    val validatorDF = Validator(this, detailLevel).validate
    //    val reportDF = rulesReport.toDS.toDF
    (validatorDF,
      true)
  }

}

object RuleSet {

  private def isGrouped(by: Seq[String]): Boolean = if (by.nonEmpty) true else false

  /**
   * Accept either a regular DataFrame or a Grouped DataFrame as the base
   * Either append rule[s] at call or via builder pattern
   */

  def apply(df: DataFrame): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(false)
  }

  def apply(df: DataFrame, by: Array[String]): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(isGrouped(by))
      .setGroupByCols(by)
  }

  //  def apply(df: RelationalGroupedDataset): RuleSet = {
  //    new RuleSet().setDF(df).setIsGrouped(true)
  //  }

  def apply(df: DataFrame, rules: Seq[Rule], by: Seq[String] = Seq.empty[String]): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(isGrouped(by))
      .setGroupByCols(by)
      .add(rules)
  }

  def apply(df: DataFrame, rules: Rule*): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(false)
      .add(rules)
  }

  //  def apply(df: RelationalGroupedDataset, rules: Rule*): RuleSet = {
  //    new RuleSet().setDF(df).setIsGrouped(true)
  //      .add(rules)
  //  }

  def generateMinMaxRules(minMaxRuleDefs: MinMaxRuleDef*): Array[Rule] = {

    minMaxRuleDefs.flatMap(ruleDef => {
      Seq(
        Rule(s"${ruleDef.ruleName}_min", min(ruleDef.column), ruleDef.bounds),
        Rule(s"${ruleDef.ruleName}_max", max(ruleDef.column), ruleDef.bounds)
      )
    }).toArray
  }

}