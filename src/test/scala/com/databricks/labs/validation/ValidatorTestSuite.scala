package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Severity
import com.databricks.labs.validation.utils.Structures.{Bounds, DateBounds, MinMaxRuleDef}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, min, to_date, array_contains, lit}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class ValidatorTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val validationValueSchema = StructType(
    StructField("validDateTime",LongType,true) ::
      StructField("validStrings",ArrayType(StringType,true),true) ::
      StructField("adhoc",StringType,true) ::
      StructField("validNumerics",ArrayType(DoubleType,true),true) ::
      StructField("bounds",ArrayType(DoubleType,false),true) ::
      StructField("blank",BooleanType,true) ::
      StructField("dateBounds",ArrayType(StringType,false),true) :: Nil
  )

  val reportSchema = StructType(
    StructField("Rule_Name",StringType,false) ::
      StructField("Rule_Type",StringType,false) ::
      StructField("Validation_Values", validationValueSchema,false) ::
      StructField("Invalid_Count",LongType,false) ::
      StructField("Failed",BooleanType,false) ::
      StructField("Rule_Severity",StringType,false) :: Nil
  )

  val groupReportSchema = StructType(
    StructField("cost",IntegerType,false) ::
      StructField("Rule_Name",StringType,false) ::
      StructField("Rule_Type",StringType,false) ::
      StructField("Validation_Values", validationValueSchema,false) ::
      StructField("Invalid_Count",LongType,false) ::
      StructField("Failed",BooleanType,false) ::
      StructField("Rule_Severity",StringType,false) :: Nil
  )

  test("The input dataframe should have no rule failures on MinMaxRule") {
    val expectedData = Seq(
      Row("MinMax_Cost_Generated_max","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Cost_Generated_min","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Cost_manual_max","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Cost_manual_min","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Cost_max","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Cost_min","bounds",Row(null,null,null,null,Array(0.0, 12.0),null,null),0L,false,Severity.fatal),
      Row("MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal)
      )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), reportSchema)

    val data = Seq()
    //  2 per rule so 2 MinMax_Sku_Price + 2 MinMax_Scan_Price + 2 MinMax_Cost + 2 MinMax_Cost_Generated
    // + 2 MinMax_Cost_manual = 10 rules
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
    )

    // Generate the array of Rules from the minmax generator
    val rulesArray = RuleSet.generateMinMaxRules(MinMaxRuleDef("MinMax_Cost_Generated", col("cost"), Bounds(0.0, 12.0), Severity.fatal))

    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    someRuleSet.addMinMaxRules("MinMax_Cost_manual", col("cost"), Bounds(0.0,12.0), Severity.fatal)
    someRuleSet.add(rulesArray)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(rulesReport.except(expectedDF).count() == 0)
    assert(passed)
    assert(rulesReport.count() == 10)
  }

  test("The input rule should have 1 invalid count for MinMax_Scan_Price_Minus_Retail_Price_min and max for failing complex type.") {
    val expectedData = Seq(
      Row("MinMax_Retail_Price_Minus_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),1L,true,Severity.fatal),
      Row("MinMax_Retail_Price_Minus_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),1L,true,Severity.fatal),
      Row("MinMax_Scan_Price_Minus_Retail_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Scan_Price_Minus_Retail_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), reportSchema)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Retail_Price_Minus_Scan_Price", col("retail_price")-col("scan_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Scan_Price_Minus_Retail_Price", col("scan_price")-col("retail_price"), Bounds(0.0, 29.99), Severity.fatal)
    )

    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(!passed)
    assert(rulesReport.count() == 4)
  }

  test("The input rule should have 3 invalid count for failing aggregate type.") {
    val expectedData = Seq(
      Row("MinMax_Min_Retail_Price","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Min_Scan_Price","bounds",Row(null,null,null,null,Array(3.0, 29.99),null,null),1L,true,Severity.fatal)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), reportSchema)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Seq(
      Rule("MinMax_Min_Retail_Price", min("retail_price"), Bounds(0.0, 29.99), Severity.fatal),
      Rule("MinMax_Min_Scan_Price", min("scan_price"), Bounds(3.0, 29.99), Severity.fatal)
    )


    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.add(minMaxPriceDefs)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(!passed)
    assert(rulesReport.count() == 2)
  }

  test("The input dataframe should have exactly 1 rule failure on MinMaxRule") {
    val expectedData = Seq(
      Row("MinMax_Cost_max","bounds",Row(null,null,null,null,Array(0.0, 12.00),null,null),1L,true,Severity.fatal),
      Row("MinMax_Cost_min","bounds",Row(null,null,null,null,Array(0.0, 12.00),null,null),0L,false,Severity.fatal),
      Row("MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row("MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), reportSchema)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 99)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0), Severity.fatal)
    )
    // Generate the array of Rules from the minmax generator

    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val (rulesReport, passed) = someRuleSet.validate()
    val failedResults  = rulesReport.filter(rulesReport("Invalid_Count") > 0).collect()
    assert(failedResults.length == 1)
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(failedResults(0)(0) == "MinMax_Cost_max")
    assert(!passed)
  }

  test("The DF in the rulesset object is the same as the input test df") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 99)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0), Severity.fatal)
    )
    // Generate the array of Rules from the minmax generator

    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val rulesDf = someRuleSet.getDf
    assert(testDF.except(rulesDf).count() == 0)
  }

  test("The group by columns are the correct group by clauses in the validation") {
    val expectedData = Seq(
      Row(3,"MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(3,"MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(3,"MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(3,"MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), groupReportSchema)
    // 2 groups so count of the rules should yield (2 minmax rules * 2 columns) * 2 groups in cost (8 rows)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 3)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99), Severity.fatal),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99), Severity.fatal)
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val (groupByValidated, passed) = someRuleSet.validate()

    assert(groupBys.length == 1)
    assert(groupBys.head == "cost")
    assert(someRuleSet.isGrouped)
    assert(passed)
    assert(groupByValidated.count() == 8)
    assert(groupByValidated.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 0)
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 0)
  }

  test("The group by columns are with rules failing the validation") {
    val expectedData = Seq(
      Row(3,"MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 0.0),null,null),1L,true,Severity.fatal),
      Row(6,"MinMax_Sku_Price_max","bounds",Row(null,null,null,null,Array(0.0, 0.0),null,null),1L,true,Severity.fatal),
      Row(3,"MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 0.0),null,null),1L,true,Severity.fatal),
      Row(6,"MinMax_Sku_Price_min","bounds",Row(null,null,null,null,Array(0.0, 0.0),null,null),1L,true,Severity.fatal),
      Row(3,"MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Scan_Price_max","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(3,"MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal),
      Row(6,"MinMax_Scan_Price_min","bounds",Row(null,null,null,null,Array(0.0, 29.99),null,null),0L,false,Severity.fatal)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), groupReportSchema)
    // 2 groups so count of the rules should yield (2 minmax rules * 2 columns) * 2 groups in cost (8 rows)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 3)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 0.0)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99))
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val (groupByValidated, passed) = someRuleSet.validate()

    assert(groupBys.length == 1, "Group by length is not 1")
    assert(groupBys.head == "cost", "Group by column is not cost")
    assert(someRuleSet.isGrouped)
    assert(!passed, "Rule set did not fail.")
    assert(groupByValidated.count() == 8, "Rule count should be 8")
    assert(groupByValidated.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 4, "Invalid count is not 4.")
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 4, "Failed count is not 4.")
  }

  test("Validate list of values with numeric types, string types and long types.") {

    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_c", 8.22, 99, 311111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")

    val numericLovExpectedData = Seq(
      Row("CheckIfCostIsInLOV","validNumerics",Row(null,null,null,Array(3.0,6.0,99.0),null,null,null),0L,false,Severity.fatal),
      Row("CheckIfScanPriceIsInLOV","validNumerics",Row(null,null,null,Array(2.51,5.11,8.22),null,null,null),0L,false,Severity.fatal),
      Row("CheckIfIdIsInLOV","validNumerics",Row(null,null,null,Array(111111111111111.0,211111111111111.0,311111111111111.0),null,null,null,null),0L,false,Severity.fatal)
    )
    val numericLovExpectedDF = spark.createDataFrame(spark.sparkContext.parallelize(numericLovExpectedData), reportSchema)

    val numericRules = Array(
      Rule("CheckIfCostIsInLOV", col("cost"), Array(3.0,6.0,99.0), Severity.fatal),
      Rule("CheckIfScanPriceIsInLOV", col("scan_price"), Array(2.51,5.11,8.22), Severity.fatal),
      Rule("CheckIfIdIsInLOV", col("id"), Array(111111111111111.0,211111111111111.0,311111111111111.0), Severity.fatal)
    )
    // Generate the array of Rules from the minmax generator

    val numericRuleSet = RuleSet(testDF)
    numericRuleSet.add(numericRules)
    val (numericValidated, numericPassed) = numericRuleSet.validate()
    assert(numericRules.map(_.ruleType == RuleType.ValidateNumerics).reduce(_ && _), "Not every value is validate numerics.")
    assert(numericRules.map(_.boundaries == null).reduce(_ && _), "Boundaries are not null.")
    assert(numericPassed)
    assert(numericValidated.except(numericLovExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(numericValidated.filter(numericValidated("Invalid_Count") > 0).count() == 0)
    assert(numericValidated.filter(numericValidated("Failed") === true).count() == 0)

    val stringRule = Rule("CheckIfProductNameInLOV", col("product_name"), Array("food_a","food_b","food_c"), Severity.fatal)
    // Generate the array of Rules from the minmax generator

    val stringLovExpectedData = Seq(
      Row("CheckIfProductNameInLOV","validStrings",Row(null,Array("food_a", "food_b", "food_c"),null,null,null,null,null),0L,false,Severity.fatal)
    )
    val stringLovExpectedDF = spark.createDataFrame(spark.sparkContext.parallelize(stringLovExpectedData), reportSchema)
    val stringRuleSet = RuleSet(testDF)
    stringRuleSet.add(stringRule)
    val (stringValidated, stringPassed) = stringRuleSet.validate()
    assert(stringRule.ruleType == RuleType.ValidateStrings)
    assert(stringRule.boundaries == null)
    assert(stringPassed)
    assert(stringValidated.except(stringLovExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(stringValidated.filter(stringValidated("Invalid_Count") > 0).count() == 0)
    assert(stringValidated.filter(stringValidated("Failed") === true).count() == 0)
  }

  test("Validate that a value must be blank or not blank with severity level set to WARN") {
    val testDF = Seq[(String, java.lang.Double, java.lang.Double, java.lang.Long)](
      ("food_a", 2.51, 3.0, null),
      ("food_b", 5.11, null, 211111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")

    val blankValidationExpectedData = Seq(
      Row("CheckIfIdIsBlank","blank",Row(null,null,null,null,null,true,null),1L,false,"WARN"),
      Row("CheckIfCostIsNotBlank","blank",Row(null,null,null,null,null,false,null),1L,false,"WARN")
    )

    val blankValidationExpectedDF = spark.createDataFrame(spark.sparkContext.parallelize(blankValidationExpectedData), reportSchema)

    val blankRules = Array(
      Rule("CheckIfIdIsBlank", col("id"), true, Severity.warning),
      Rule("CheckIfCostIsNotBlank", col("cost"), false, Severity.warning)
    )

    val blankRuleSet = RuleSet(testDF)
    blankRuleSet.add(blankRules)
    val (blankValidated, blankPassed) = blankRuleSet.validate()

    assert(blankRules.map(_.ruleType == RuleType.ValidateBlank).reduce(_ && _), "Not every value is validate blank.")
    assert(blankPassed)
    assert(blankValidated.except(blankValidationExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(blankValidated.filter(blankValidated("Invalid_Count") > 0).count() == 2)
    assert(blankValidated.filter(blankValidated("Failed") === true).count() == 0)
  }

  test("Validate that a date value is comprised within the given date bounds") {
    val tmpTestDF = Seq(
      ("1234", "2019-08-12"),
      ("1235", "2000-03-23")
    ).toDF("invoice_id", "date_received")
    val testDF = tmpTestDF.withColumn("date_received", to_date($"date_received"))

    val dateBoundsValidationExpectedData = Seq(
      Row("CheckIfDateIsValid","dateBounds",Row(null,null,null,null,null,null,Array("2019-01-01","2019-12-31")),1L,true,"FATAL")
    )

    val dateBoundsValidationExpectedDF = spark.createDataFrame(spark.sparkContext.parallelize(dateBoundsValidationExpectedData), reportSchema)

    val dateBoundsRules = Array(
      Rule("CheckIfDateIsValid", col("date_received"), DateBounds("2019-01-01", "2019-12-31"), Severity.fatal)
    )

    val dateBoundsRuleSet = RuleSet(testDF)
    dateBoundsRuleSet.add(dateBoundsRules)
    val (dateBoundsValidated, dateBoundsPassed) = dateBoundsRuleSet.validate()

    assert(dateBoundsRules.map(_.ruleType == RuleType.ValidateDateBounds).reduce(_ && _), "Not every value is validate blank.")
    assert(!dateBoundsPassed)
    assert(dateBoundsValidated.except(dateBoundsValidationExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(dateBoundsValidated.filter(dateBoundsValidated("Invalid_Count") > 0).count() == 1)
    assert(dateBoundsValidated.filter(dateBoundsValidated("Failed") === true).count() == 1)
  }

  test("Validate value against adhoc validation") {
    val testDF = Seq(
      ("name_convention.123", "red"),
      ("bad_name_convention.suffix", "blue")
    ).toDF("name", "color")

    val adhocValidationExpectedData = Seq(
      Row("CheckIfNameMatchesConvention","adhoc",Row(null,null,"name RLIKE ^\\w+_\\w+.\\d+$",null,null,null,null),1L,true,"FATAL"),
      Row("CheckIfColorIsValid","adhoc",Row(null,null,"array_contains([yellow,green,red], color)",null,null,null,null),1L,true,"FATAL")
    )

    val adhocValidationExpectedDF = spark.createDataFrame(spark.sparkContext.parallelize(adhocValidationExpectedData), reportSchema)

    val adhocRules = Array(
      Rule("CheckIfNameMatchesConvention", col("name").rlike("^\\w+_\\w+.\\d+$"), Severity.fatal),
      Rule("CheckIfColorIsValid", array_contains(lit(Array("yellow", "green", "red")), col("color")), Severity.fatal)
    )

    val adhocRuleSet = RuleSet(testDF)
    adhocRuleSet.add(adhocRules)
    val (adhocValidated, adhocPassed) = adhocRuleSet.validate()

    assert(adhocRules.map(_.ruleType == RuleType.ValidateAdhoc).reduce(_ && _), "Not every value is validate blank.")
    assert(!adhocPassed)
    assert(adhocValidated.except(adhocValidationExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(adhocValidated.filter(adhocValidated("Invalid_Count") > 0).count() == 2)
    assert(adhocValidated.filter(adhocValidated("Failed") === true).count() == 2)
  }
}
