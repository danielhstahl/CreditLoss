package com.danielhstahl.creditloss
import org.apache.spark.sql.{SparkSession}
import org.scalatest._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.{lit}
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
        (u: Complex, l: Double, lgdVariance: Double) => (u * l).exp,
        LoanLoss.getLiquidityRiskFn(50.0, 0.0)
      )
    print(portfolioMetrics.cf)
    assert(portfolioMetrics.cf.length === 256 * 3)
    assert(portfolioMetrics.el.length === 3)
    assert(portfolioMetrics.variance.length === 3)

    //assert(results === expected)
  }
}
