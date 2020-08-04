package com.danielhstahl.creditloss
import org.apache.spark.sql.{SparkSession, DataFrame, Row, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{
  StructType,
  ArrayType,
  DoubleType,
  StringType,
  StructField,
  LongType,
  DataType
}
import breeze.math.Complex
import scala.util.Random
import scala.math
import org.apache.spark.sql.functions.{udf, col, sum}

object LoanLossSim {
  val balanceRange = (1000.0, 1000000.0)
  val pdCategories =
    List(0.0001, 0.0002, 0.0005, .001, .003, .006, 0.01, 0.04, 0.1)
  val lgdCategories = List(0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1)
  val rCategories = List(0.0, 0.2, 0.4)
  val lgdVarianceCategories = List(0.1, 0.2, 0.3, 0.5)
  val weightCategories = List(
    Array(0.3, 0.5, 0.2),
    Array(0.2, 0.2, 0.6),
    Array(0.1, 0.6, 0.3),
    Array(0.4, 0.2, 0.4)
  )
  val schema = StructType(
    Seq(
      StructField("balance", DoubleType, false),
      StructField("pd", DoubleType, false),
      StructField("lgd", DoubleType, false),
      StructField("weight", ArrayType(DoubleType, false), false),
      StructField("r", DoubleType, true),
      StructField("lgd_variance", DoubleType, true)
    )
  )
  def simulateRange(min: Double, max: Double, rand: Random): Double = {
    min + (max - min) * rand.nextDouble
  }
  def simulateCategory[A](categories: List[A], rand: Random): A = {
    categories(rand.nextInt(categories.length))
  }
  def simulatePortfolio(
      spark: SparkSession,
      numLoans: Int,
      seed: Int
  ): DataFrame = {
    val r = new Random(seed)
    val rows = (1 to numLoans).map(v => simulateLoan(r))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }
  def simulateLoan(rand: Random): Row = {
    Row(
      simulateRange(balanceRange._1, balanceRange._2, rand),
      simulateCategory(pdCategories, rand),
      simulateCategory(lgdCategories, rand),
      simulateCategory(weightCategories, rand),
      simulateCategory(rCategories, rand),
      simulateCategory(lgdVarianceCategories, rand)
    )
  }
}

case class PortfolioMetrics(
    cf: Seq[Complex],
    expectation: Double,
    variance: Double,
    xMin: Double
)
case class PortfolioMoments(
    el: Seq[Double],
    variance: Seq[Double],
    lambda: Double
)
class LoanLoss(
    q: Double,
    lambda: Double,
    systemicExpectation: Seq[Double],
    systemicVariance: Seq[Double]
) {

  val numWeight = systemicExpectation.length
  //var cf: Seq[Complex] = Seq()
  /*private var loanEl: Seq[Double] = Seq()
  private var loanVariance: Seq[Double] = Seq()
  private var loanLambda: Double = 0.0*/
  //var xMin: Double = 0.0
  //var variance = 0.0
  //var expectation = 0.0
  private[this] def chebyschev(
      expectation: Double,
      variance: Double
  ): Double = {
    val k = 30.0 // represents 99.5% probability of being below
    expectation - k * math.sqrt(variance)
  }
  private[this] def nonLiquidityExpectation(
      loanEl: Seq[Double],
      systemicExpectation: Seq[Double]
  ): Double = {
    (loanEl, systemicExpectation).zipped.map(_ * _).sum
  }
  private[this] def nonLiquidityVariance(
      loanEl: Seq[Double],
      loanVariance: Seq[Double],
      systemicExpectation: Seq[Double],
      systemicVariance: Seq[Double]
  ): Double = {
    val ep = (loanEl, systemicVariance).zipped
      .map((a, b) => a * a * b)
      .sum
    val vp = (loanVariance, systemicExpectation).zipped.map(_ * _).sum
    vp + ep
  }
  private[this] def portfolioVariancePure(
      q: Double,
      totalLambda: Double,
      nlExpectation: Double,
      nlVariance: Double
  ): Double = {
    val mQL = 1.0 + q * totalLambda
    nlVariance * mQL * mQL - nlExpectation * q * totalLambda * totalLambda
  }
  private[this] def portfolioExpectationPure(
      q: Double,
      totalLambda: Double,
      nlExpectation: Double
  ): Double = {
    val mQL = 1.0 + q * totalLambda
    nlExpectation * mQL
  }

  private[this] def getLiquidityRiskFn(
      q: Double,
      totalLambda: Double
  ): (Complex) => Complex = { (u: Complex) =>
    u - ((-u * totalLambda).exp - 1.0) * q
  }
  private[this] def getLogLpmCf(
      lgdCf: (Complex, Double, Double) => Complex,
      liquidityCf: (Complex) => Complex
  ): (Complex, Double, Double, Double, Double) => Complex = {
    (
        u: Complex,
        balance: Double,
        pd: Double,
        lgd: Double,
        lgdVariance: Double
    ) => (lgdCf(liquidityCf(u), lgd * balance, lgdVariance) - 1.0) * pd
  }
  /*
  def getRiskContributionForLoan(
      balance: Double,
      lgd: Double,
      pd: Double,
      weight: Seq[Double],
      r: Double,
      lgdVariance: Double,
      c: Double //scalar for multiplying covariance.  Typically (rho(X)-E[X])/sqrt(Var(X))
  ): Double = {
    val elScalarIncremental = 1.0 + q * lambda
    val elScalarTotal = q * SparkMethods.getLambdaFromLoan(balance, r, 1)
    val nlExpectation = nonLiquidityExpectation(loanEl, systemicExpectation)
    val nlVariance = nonLiquidityVariance(
      loanEl,
      loanVariance,
      systemicExpectation,
      systemicVariance
    )
    val portfolioStandardDeviation = math.sqrt(portfolioVariance())
    val varianceScalarIncremental = elScalarIncremental * elScalarIncremental
    val varianceScalarTotal =
      elScalarTotal * (2.0 * elScalarIncremental + q * loanLambda)
    val varianceElTotal = elScalarTotal * (2.0 * lambda + loanLambda)
    val expectationIncremental =
      (
        systemicExpectation,
        SparkMethods.getElFromLoan(balance, lgd, pd, weight, 1)
      ).zipped
        .map(_ * _)
        .sum
    val varianceIncremental = (
      systemicExpectation,
      SparkMethods.getVarFromLoan(balance, lgd, pd, weight, lgdVariance, 1)
    ).zipped
      .map(_ * _)
      .sum + (
      systemicVariance,
      SparkMethods.getElFromLoan(balance, lgd, pd, weight, 1),
      loanEl
    ).zipped.map(_ * _ * _).sum
    elScalarIncremental * expectationIncremental + elScalarTotal * nlExpectation + c * (varianceScalarIncremental * varianceIncremental + varianceScalarTotal * nlVariance - expectationIncremental * q * lambda * lambda - nlExpectation * varianceElTotal) / portfolioStandardDeviation
  }*/

  private[this] def getMoments(loanDF: DataFrame) = {
    val elUDF = udf(SparkMethods.getElFromLoan _)
    val varUDF = udf(SparkMethods.getVarFromLoan _)
    val lambdaUDF = udf(SparkMethods.getLambdaFromLoan _)
    val doubleSum = new SumDoubleElement
    val results = loanDF
      .withColumn(
        "el",
        elUDF(col("balance"), col("lgd"), col("pd"), col("weight"), col("num"))
      )
      .withColumn(
        "variance",
        varUDF(
          col("balance"),
          col("lgd"),
          col("pd"),
          col("weight"),
          col("lgd_variance"),
          col("num")
        )
      )
      .withColumn("lambda", lambdaUDF(col("balance"), col("r"), col("num")))
      .agg(
        doubleSum(col("el")).alias("el"),
        doubleSum(col("variance")).alias("variance"),
        sum("lambda").alias("lambda")
      )
      .collect()
      .map({
        case Row(
              el: Seq[Double],
              variance: Seq[Double],
              lambda: Double
            ) =>
          PortfolioMoments(
            el,
            variance,
            lambda
          )
      })
      .head
    val nlE = nonLiquidityExpectation(results.el, systemicExpectation)
    val variance = portfolioVariancePure(
      q,
      results.lambda + lambda,
      nlE,
      nonLiquidityVariance(
        results.el,
        results.variance,
        systemicExpectation,
        systemicVariance
      )
    )
    val expectation = portfolioExpectationPure(q, results.lambda + lambda, nlE)
    val pXMin = chebyschev(expectation, variance)
    (expectation, variance, results.lambda, pXMin)
  }
  def getPortfolioMetrics(
      loanDF: DataFrame,
      numU: Int,
      lgdCf: (Complex, Double, Double) => Complex
  ): PortfolioMetrics = {
    val (expectation, variance, pLambda, xMin) = getMoments(
      loanDF
    )
    val uDomain = FangOost.getUDomain(numU, xMin, 0.0)
    val logCf =
      getLogLpmCf(lgdCf, getLiquidityRiskFn(q, lambda + pLambda))
    val logLpmCFUDF =
      udf((balance: Double, pd: Double, lgd: Double, lgdVariance: Double) =>
        uDomain
          .map(u => logCf(u, balance, pd, lgd, lgdVariance))
          .map(ucf => {
            Seq(ucf.re, ucf.im)
          })
      )

    val cfUDF = udf(SparkMethods.getCFForLoan _)
    val complexSum = new SumArrayElement
    val cf = loanDF
      .withColumn(
        "logCf",
        logLpmCFUDF(col("balance"), col("pd"), col("lgd"), col("lgd_variance"))
      )
      .withColumn(
        "cf",
        cfUDF(
          col("logCf"),
          col("weight"),
          col("num")
        )
      )
      .agg(
        complexSum(col("cf")).alias("cf")
      )
      .collect()
      .map({
        case Row(
              cf: Seq[Seq[Double]]
            ) =>
          cf.map(v => Complex(v(0), v(1)))
      })
      .head
    PortfolioMetrics(cf, expectation, variance, xMin)

  }
  def getFullCF(
      cf: Seq[Complex],
      mgf: (Seq[Double], Seq[Double]) => (Seq[Complex]) => Complex
  ): Seq[Complex] = {
    cf.grouped(numWeight).map(mgf(systemicExpectation, systemicVariance)).toSeq
  }

}

object SparkMethods {
  def getCFForLoan(
      uCf: Seq[Seq[Double]],
      weight: Seq[Double],
      num: Int
  ): Seq[Seq[Double]] = {

    (1 to weight.length * uCf.length).map(i => {
      val rowNum = VecToMat.getRowFromIndex(i - 1, weight.length)
      val colNum = VecToMat.getColFromIndex(i - 1, weight.length)
      val cmp = uCf(colNum)
      val scalar = weight(rowNum) * num
      Seq(
        cmp(0) * scalar,
        cmp(1) * scalar
      ) //note that I'm doing complex multiplication here
    })
  }
  def getElFromLoan(
      balance: Double,
      lgd: Double,
      pd: Double,
      weight: Seq[Double],
      num: Int
  ): Seq[Double] = {
    weight.map(w => (-lgd * balance * w * pd * num))
  }
  def getVarFromLoan(
      balance: Double,
      lgd: Double,
      pd: Double,
      weight: Seq[Double],
      lgdVariance: Double,
      num: Int
  ): Seq[Double] = {
    val l = lgd * balance
    weight.map(w => (1.0 + lgdVariance) * l * l * w * pd * num)
  }
  def getLambdaFromLoan(
      balance: Double,
      r: Double,
      num: Int
  ): Double = {
    balance * r * num
  }
}
