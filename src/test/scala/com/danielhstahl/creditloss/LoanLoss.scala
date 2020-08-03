package com.danielhstahl.creditloss
import org.apache.spark.sql.{SparkSession}
import org.scalatest._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  StructType,
  ArrayType,
  DoubleType,
  StringType,
  StructField,
  IntegerType,
  DataType
}
import breeze.math.Complex
class LoanLossDistribution extends FunSuite with DataFrameSuiteBase {
  test("it aggregates correctly") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val loanDF =
      LoanLossSim.simulatePortfolio(spark, 30, 42).withColumn("num", lit(1))
    val portfolioMetrics =
      LoanLoss.getPortfolioMetrics(
        loanDF,
        256,
        -30 * 1000000.0,
        50.0,
        0.0,
        (u: Complex, l: Double, lgdVariance: Double) => (u * l).exp
      )
    assert(portfolioMetrics.cf.length === 256 * 3)
    assert(portfolioMetrics.el.length === 3)
    assert(portfolioMetrics.variance.length === 3)
  }
  test("gets correct expectation") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val balance1 = 50000.0
    val balance2 = 500000.0
    val balance3 = 100000.0
    val lgd1 = 0.3
    val lgd2 = 0.6
    val lgd3 = 0.8
    var r = 0.2
    val num1 = 100
    val num2 = 1000
    val num3 = 100
    val pd1 = 0.01
    val pd2 = 0.0005
    val pd3 = 0.006
    val rows = Seq(
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), r, 0.2, num1),
      Row(balance2, pd2, lgd2, Array(0.3, 0.5, 0.2), r, 0.2, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), r, 0.2, num3)
    )
    val schema = StructType(
      Seq(
        StructField("balance", DoubleType, false),
        StructField("pd", DoubleType, false),
        StructField("lgd", DoubleType, false),
        StructField("weight", ArrayType(DoubleType, false), false),
        StructField("r", DoubleType, true),
        StructField("lgd_variance", DoubleType, true),
        StructField("num", IntegerType, true)
      )
    )
    val loanDF = spark.createDataFrame(
      spark.sparkContext.parallelize(
        rows
      ),
      schema
    )
    val expectedPortfolioLoss =
      (-balance1 * lgd1 * pd1 * num1 - balance2 * lgd2 * pd2 * num2 - balance3 * lgd3 * pd3 * num3) //no lambda
    val xMin = expectedPortfolioLoss * 10
    val portfolioMetrics = LoanLoss.getPortfolioMetrics(
      loanDF,
      256,
      xMin,
      50.0, //lambda
      0.0, //q
      LgdCF.degenerateCf
    )
    val cf = LoanLoss.getFullCF(
      3,
      portfolioMetrics.cf,
      SystemicVariableCF.degenerateMgf
    )
    val el = portfolioMetrics.el.sum
    val distEl = FangOost.getExpectation(xMin, 0.0, cf)
    println(el)
    println(distEl)
    println(expectedPortfolioLoss)
    assert(math.abs(el - distEl) < 1000)
    assert(el === expectedPortfolioLoss)

  }
  test("gets correct variance") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val balance1 = 50000.0
    val balance2 = 500000.0
    val balance3 = 100000.0
    val lgd1 = 0.3
    val lgd2 = 0.6
    val lgd3 = 0.8
    var r = 0.2
    val num1 = 100
    val num2 = 1000
    val num3 = 100
    val pd1 = 0.01
    val pd2 = 0.0005
    val pd3 = 0.006
    val rows = Seq(
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), r, 0.2, num1),
      Row(balance2, pd2, lgd2, Array(0.3, 0.5, 0.2), r, 0.2, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), r, 0.2, num3)
    )
    val schema = StructType(
      Seq(
        StructField("balance", DoubleType, false),
        StructField("pd", DoubleType, false),
        StructField("lgd", DoubleType, false),
        StructField("weight", ArrayType(DoubleType, false), false),
        StructField("r", DoubleType, true),
        StructField("lgd_variance", DoubleType, true),
        StructField("num", IntegerType, true)
      )
    )
    val loanDF = spark.createDataFrame(
      spark.sparkContext.parallelize(
        rows
      ),
      schema
    )
    val expectedPortfolioLoss =
      (-balance1 * lgd1 * pd1 * num1 - balance2 * lgd2 * pd2 * num2 - balance3 * lgd3 * pd3 * num3) //no lambda
    val xMin = expectedPortfolioLoss * 10
    val portfolioMetrics = LoanLoss.getPortfolioMetrics(
      loanDF,
      256,
      xMin,
      50.0, //lambda
      0.0, //q
      LgdCF.degenerateCf
    )
    val mgfcf = SystemicVariableCF.gammaMgf(Seq(0.3, 0.3, 0.3))
    val cf = LoanLoss.getFullCF(
      3,
      portfolioMetrics.cf,
      mgfcf
    )
    val el = portfolioMetrics.el.sum
    val distEl = FangOost.getExpectation(xMin, 0.0, cf)
    println(el)
    println(distEl)
    println(expectedPortfolioLoss)

    val variance = LoanLoss.portfolioVariance(
      portfolioMetrics.el,
      Seq(1.0, 1.0, 1.0),
      portfolioMetrics.variance,
      Seq(0.3, 0.3, 0.3)
    )
    val distVar =
      FangOost.getVarianceWithExpectation(xMin, 0.0, expectedPortfolioLoss, cf)
    println(variance)
    println(distVar)
    assert(math.abs(variance - distVar) < 1000)
    assert(el === expectedPortfolioLoss)

  }
}
