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
    val ll = new LoanLoss(
      50.0,
      0.0,
      Seq(1.0, 1.0, 1.0),
      Seq(0.3, 0.3, 0.3)
    )
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      (u: Complex, l: Double, lgdVariance: Double) => (u * l).exp
    )
    assert(metrics.cf.length === 256 * 3)
  }
  test("gets correct expectation lgdvariance>0 r=0") {
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
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), 0.0, 0.2, num1),
      Row(balance2, pd2, lgd2, Array(0.3, 0.5, 0.2), 0.0, 0.2, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), 0.0, 0.2, num3)
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
    //val xMin = expectedPortfolioLoss * 10
    val ll =
      new LoanLoss(
        0.01 / -expectedPortfolioLoss,
        0.0,
        Seq(1.0, 1.0, 1.0),
        Seq(0.3, 0.3, 0.3)
      )
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.degenerateCf
    )
    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.degenerateMgf
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)
    println("gets correct expectation")
    println(el)
    println(distEl)
    println(expectedPortfolioLoss)
    println("end gets correct expectation")
    assert(math.abs(el / distEl - 1.0) < 0.001)
    assert(el === expectedPortfolioLoss)

  }

  test("gets correct expectation with lgdvariance>0 r>0") {
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
    val num1 = 10000
    val num2 = 10000
    val num3 = 10000
    val pd1 = 0.01
    val pd2 = 0.0005
    val pd3 = 0.006
    val rows = Seq(
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), 0.2, 0.2, num1),
      Row(balance2, pd2, lgd2, Array(0.3, 0.5, 0.2), 0.2, 0.2, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), 0.2, 0.2, num3)
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
    //val xMin = expectedPortfolioLoss * 10
    val ll =
      new LoanLoss(
        0.01 / -expectedPortfolioLoss,
        0.0,
        Seq(1.0, 1.0, 1.0),
        Seq(0.3, 0.3, 0.3)
      )
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.degenerateCf
    )
    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.degenerateMgf
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)
    println("gets correct expectation")
    println(el)
    println(distEl)
    println(expectedPortfolioLoss)
    println("end gets correct expectation")
    assert(math.abs(el / distEl - 1.0) < 0.001)
    assert(el === expectedPortfolioLoss)

  }
  test("gets correct variance r>0 lgdvariance>0") {
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
    val num1 = 10000
    val num2 = 10000
    val num3 = 10000
    val pd1 = 0.01
    val pd2 = 0.0005
    val pd3 = 0.006
    val lgdVariance = 0.2
    val rows = Seq(
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), r, lgdVariance, num1),
      Row(balance2, pd2, lgd2, Array(0.4, 0.3, 0.3), r, lgdVariance, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), r, lgdVariance, num3)
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
      (-balance1 * lgd1 * pd1 * num1 - balance2 * lgd2 * pd2 * num2 - balance3 * lgd3 * pd3 * num3)
    val totalExpPortLoss =
      expectedPortfolioLoss - 0.0001 * r * (num1 * balance1 + num2 * balance2 + num3 * balance3)
    val ll =
      new LoanLoss(
        0.0001 / -expectedPortfolioLoss,
        0.0,
        Seq(1.0, 1.0, 1.0),
        Seq(0.3, 0.3, 0.3)
      )
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.degenerateCf
    )
    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v)
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)
    println("gets correct variance")
    println(el)
    println(totalExpPortLoss)
    println(distEl)
    val variance = metrics.variance
    val distVar =
      FangOost.getVarianceWithExpectation(
        metrics.xMin,
        0.0,
        expectedPortfolioLoss,
        cf
      )
    println(variance)
    println(distVar)
    println("end gets correct variance")
    assert(math.abs(variance / distVar - 1.0) < 0.001)
    assert(el === expectedPortfolioLoss)

  }
  test("gets correct variance r=0 lgdvariance>0") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val balance1 = 50000.0
    val balance2 = 500000.0
    val balance3 = 100000.0
    val lgd1 = 0.3
    val lgd2 = 0.6
    val lgd3 = 0.8
    var r = 0.0
    val num1 = 10000
    val num2 = 10000
    val num3 = 10000
    val pd1 = 0.01
    val pd2 = 0.0005
    val pd3 = 0.006
    val lgdVariance = 0.2
    val rows = Seq(
      Row(balance1, pd1, lgd1, Array(0.3, 0.5, 0.2), r, lgdVariance, num1),
      Row(balance2, pd2, lgd2, Array(0.4, 0.3, 0.3), r, lgdVariance, num2),
      Row(balance3, pd3, lgd3, Array(0.2, 0.2, 0.6), r, lgdVariance, num3)
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
      (-balance1 * lgd1 * pd1 * num1 - balance2 * lgd2 * pd2 * num2 - balance3 * lgd3 * pd3 * num3)
    val totalExpPortLoss =
      expectedPortfolioLoss - 0.0001 * r * (num1 * balance1 + num2 * balance2 + num3 * balance3)
    val ll =
      new LoanLoss(
        0.0001 / -expectedPortfolioLoss,
        0.0,
        Seq(1.0, 1.0, 1.0),
        Seq(0.3, 0.3, 0.3)
      )
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.degenerateCf
    )
    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v)
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)
    println("gets correct variance")
    println(el)
    println(totalExpPortLoss)
    println(distEl)
    val variance = metrics.variance
    val distVar =
      FangOost.getVarianceWithExpectation(
        metrics.xMin,
        0.0,
        expectedPortfolioLoss,
        cf
      )
    println(variance)
    println(distVar)
    println("end gets correct variance")
    assert(math.abs(variance / distVar - 1.0) < 0.001)
    assert(el === expectedPortfolioLoss)

  }
  test("gets correct variance homogenous lgdvariance>0 r=0") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val balance = 1.0
    val pd = 0.05
    val lgd = 0.5
    val numLoans = 10000
    val lambda = 1000
    val q = 0.01 / (numLoans * pd * lgd * balance)
    //val xMin = (-numLoans * pd * lgd * balance - lambda) * 3.0
    val weight = Seq(0.4, 0.6)
    val rows = Seq(
      Row(balance, pd, lgd, weight, 0.0, 0.2, numLoans)
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
    val ll = new LoanLoss(q, lambda, Seq(1.0, 1.0), Seq(0.4, 0.3))
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.gammaCf
    )

    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v)
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)

    val variance = metrics.variance

    val distVar =
      FangOost.getVarianceWithExpectation(metrics.xMin, 0.0, el, cf)
    println("gets correct variance same rust")
    println(variance)
    println(distVar)
    println("end gets correct variance same rust")
    assert(math.abs(variance / distVar - 1.0) < 0.001)
    assert(math.abs(el - distEl) < 0.0001)

  }
  test("gets correct variance non-homegenous lgdvariance>0 r=0") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val balance1 = 1.0
    val pd1 = 0.05
    val lgd1 = 0.5
    val balance2 = 1.5
    val pd2 = 0.03
    val lgd2 = 0.6
    val numLoans = 5000
    val lambda = 1000
    val q = 0.01 / (numLoans * pd1 * lgd1 * balance1 * 2)
    //val xMin = (-numLoans * pd1 * lgd1 * balance1 * 2 - lambda) * 3.0
    val weight1 = Seq(0.4, 0.6)
    val weight2 = Seq(0.3, 0.7)
    val rows = Seq(
      Row(balance1, pd1, lgd1, weight1, 0.0, 0.2, numLoans),
      Row(balance2, pd2, lgd2, weight2, 0.0, 0.2, numLoans)
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
    val ll = new LoanLoss(q, lambda, Seq(1.0, 1.0), Seq(0.4, 0.3))
    val metrics = ll.getPortfolioMetrics(
      loanDF,
      256,
      LgdCF.gammaCf
    )

    val cf = ll.getFullCF(
      metrics.cf,
      (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v)
    )
    val el = metrics.expectation
    val distEl = FangOost.getExpectation(metrics.xMin, 0.0, cf)

    val variance = metrics.variance

    val distVar =
      FangOost.getVarianceWithExpectation(metrics.xMin, 0.0, el, cf)
    println("gets correct variance non homgenous rust")
    println(variance)
    println(distVar)
    println("end gets correct variance non homgenous rust")
    assert(math.abs(variance / distVar - 1.0) < 0.001)
    assert(math.abs(el - distEl) < 0.0001)

  }
}
