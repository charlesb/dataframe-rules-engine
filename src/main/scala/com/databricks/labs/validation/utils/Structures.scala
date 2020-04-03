package com.databricks.labs.validation.utils

import com.databricks.labs.validation.utils.Structures.{ValidNumerics, ValidStrings}
import org.apache.spark.sql.Column

object Lookups {
  final val validStoreIDs = ValidNumerics("store_id", Array(1001, 1002))

  final val validStrings = ValidStrings("region",
    Array("Northeast", "Southeast", "Midwest", "Northwest", "Southcentral", "Southwest")
  )

  final val validSkus = ValidNumerics("sku",
    Array(123456, 122987,123256, 173544, 163212, 365423, 168212))


//  val df = sc.parallelize(Seq(
//    ("Northwest", 1001, 123456, 9.32, 8.99, 4.23),
//    ("Northwest", 1001, 123256, 19.99, 16.49, 12.99),
//    ("Northwest", 1001, 123456, 0.99, 0.99, 0.10),
//    ("Northwest", 1001, 123456, 0.98, 0.90, 0.10), // non_distinct sku
//    ("Northwest", 1002, 122987, 9.99, 9.49, 6.49),
//    ("Northwest", 1002, 173544, 1.29, 0.99, 1.23),
//    ("Northwest", 1002, 168212, 3.29, 1.99, 1.23),
//    ("Northwest", 1002, 365423, 1.29, 0.99, 1.23),
//    ("Northwest", 1002, 3897615, 14.99, 129.99, 1.23),
//    ("Northwest", 1003, 163212, 3.29, 1.99, 1.23) // Invalid numeric store_id groupby test
}

object Structures {

  case class Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity)

  case class Result(ruleName: String, groupId: String, boundaries: Bounds, failedCount: Long, passed: Boolean)

  case class MinMaxRuleDef(ruleName: String, column: Column, bounds: Bounds, by: Column*)

  case class ValidNumerics(columnName: String, valid: Array[Double])
  case class ValidStrings(columnName: String, valid: Array[String])

}
