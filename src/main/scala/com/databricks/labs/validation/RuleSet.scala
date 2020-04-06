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
  private var _isGrouped: Boolean = false
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
    _isGrouped = true
    this
  }

  private[validation] def getDf: DataFrame = _df

  private[validation] def isGrouped: Boolean = _isGrouped

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
      .setIsGrouped(ruleSet.isGrouped)
      .add(ruleSet.getRules)
  }

  /**
   * Logic to test compliance with provided rules added through the builder
   * TODO What's missed
   * @return this but is marked private
   */
  private def validateRules(): Unit = {
    require(getRules.map(_.ruleName).distinct.length == getRules.map(_.ruleName).length,
      s"Duplicate Rule Names: ${getRules.map(_.ruleName).diff(getRules.map(_.ruleName).distinct).mkString(", ")}")
  }

  /**
   * Call the action once all rules have been applied
   * @param detailLevel -- For Future -- Perhaps faster way to just return true/false without
   *                    processing everything and returning a report. For big data sets, perhaps run samples
   *                    looking for invalids? Not sure how much faster and/or what the break-even would be
   * @return Tuple of Dataframe report and final boolean of whether all rules were passed
   */
  def validate(detailLevel: Int = 1): (DataFrame, Boolean) = {
    validateRules()
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
    new RuleSet().setDF(df)
  }

  def apply(df: DataFrame, by: Array[String]): RuleSet = {
    new RuleSet().setDF(df)
      .setGroupByCols(by)
  }

  def apply(df: DataFrame, rules: Seq[Rule], by: Seq[String] = Seq.empty[String]): RuleSet = {
    new RuleSet().setDF(df)
      .setGroupByCols(by)
      .add(rules)
  }

  def apply(df: DataFrame, rules: Rule*): RuleSet = {
    new RuleSet().setDF(df)
      .add(rules)
  }

  def generateMinMaxRules(minMaxRuleDefs: MinMaxRuleDef*): Array[Rule] = {

    minMaxRuleDefs.flatMap(ruleDef => {
      Seq(
        Rule(s"${ruleDef.ruleName}_min", min(ruleDef.column), ruleDef.bounds),
        Rule(s"${ruleDef.ruleName}_max", max(ruleDef.column), ruleDef.bounds)
      )
    }).toArray
  }

}