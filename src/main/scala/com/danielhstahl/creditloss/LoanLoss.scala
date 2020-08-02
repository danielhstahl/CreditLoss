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
    spark.createDataFrame(spark.sparkContext.parallelize(rows), LoanLoss.schema)
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
    el: Seq[Double],
    variance: Seq[Double],
    lambda: Double
)
object LoanLoss {
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
    weight.map(w =>
      (1.0 + lgdVariance) * math.pow(lgd * balance, 2.0) * w * pd * num
    )
  }
  def getLambdaFromLoan(balance: Double, r: Double, num: Int): Double = {
    balance * r * num
  }
  def portfolioExpectation(
      portfolioEl: Seq[Double],
      systemicEl: Seq[Double]
  ): Double = {
    (portfolioEl, systemicEl).zipped.map(_ * _).sum
  }
  def portfolioVariance(
      portfolioEl: Seq[Double],
      systemicEl: Seq[Double],
      portfolioVariance: Seq[Double],
      systemicVariance: Seq[Double]
  ): Double = {
    val ep = (portfolioEl, systemicVariance).zipped
      .map((a, b) => math.pow(a, 2.0) * b)
      .sum
    val vp = (portfolioVariance, systemicEl).zipped.map(_ * _).sum
    vp + ep
  }
  def portfolioVarianceWithLiquidity(
      lambda: Double,
      q: Double,
      portfolioExpectation: Double,
      portfolioVariance: Double
  ): Double = {
    portfolioVariance * math.pow(
      1.0 + q * lambda,
      2.0
    ) - portfolioExpectation * q * lambda * lambda
  }
  def portfolioExpectationWithLiquidity(
      lambda: Double,
      q: Double,
      expectation: Double
  ): Double = {
    expectation * (1.0 + q * lambda)
  }
  def getLiquidityRiskFn(lambda: Double, q: Double): (Complex) => Complex = {
    (u: Complex) => u - ((-u * lambda).exp - 1.0) * q
  }
  def getLogLpmCf(
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

  def getRiskContributionForLoan(
      balance: Double,
      lgd: Double,
      pd: Double,
      weight: Seq[Double],
      r: Double,
      lgdVariance: Double,
      num: Int,
      portfolioElByWeight: Seq[Double],
      systemicEl: Seq[Double], //typically list of ones
      portfolioVarianceByWeight: Seq[Double],
      systemicVariance: Seq[Double],
      lambda0: Double, //base loss per liquidity event
      lambda: Double, //sum of r*balance over all loans
      q: Double, //probability of liquidity event, scaled by total portfolio loss
      c: Double //scalar for multiplying covariance.  Typically (rho(X)-E[X])/sqrt(Var(X))
  ): Double = {
    val elScalarIncremental = 1.0 + q * lambda0
    val elScalarTotal = q * getLambdaFromLoan(balance, r, num)
    val expectationTotal = portfolioExpectation(portfolioElByWeight, systemicEl)
    val varianceTotal = portfolioVariance(
      portfolioElByWeight,
      systemicEl,
      portfolioVarianceByWeight,
      systemicVariance
    )
    val portfolioStandardDeviation = portfolioVarianceWithLiquidity(
      lambda,
      q,
      expectationTotal,
      varianceTotal
    )
    val varianceScalarIncremental = elScalarIncremental * elScalarIncremental
    val varianceScalarTotal =
      elScalarTotal * (2.0 * elScalarIncremental + q * lambda)
    val varianceElTotal = elScalarTotal * (2.0 * lambda0 + lambda)
    val expectationIncremental =
      (systemicEl, getElFromLoan(balance, lgd, pd, weight, num)).zipped
        .map(_ * _)
        .sum
    val varianceIncremental = (
      systemicEl,
      getVarFromLoan(balance, lgd, pd, weight, lgdVariance, num)
    ).zipped
      .map(_ * _)
      .sum
    elScalarIncremental * expectationIncremental + elScalarTotal * expectationTotal + c * (varianceScalarIncremental * varianceIncremental + varianceScalarTotal * varianceTotal - expectationIncremental * q * lambda0 * lambda0 - expectationTotal * varianceElTotal) / portfolioStandardDeviation
  }

  def getCFForLoan(
      uCf: Seq[Seq[Double]],
      weight: Seq[Double],
      num: Int
  ): Seq[Seq[Double]] = {

    (1 to weight.length * uCf.length).map(i => {
      val rowNum = VecToMat.getRowFromIndex(i - 1, weight.length)
      val colNum = VecToMat.getColFromIndex(i - 1, weight.length)
      val cmp = uCf(colNum)
      Seq(
        cmp(0) * weight(rowNum) * num,
        cmp(1)
      ) //note that I'm doing complex multiplication here
    })
  }
  def getPortfolioMetrics(
      loanDF: DataFrame,
      numU: Int,
      xMin: Double,
      lgdCf: (Complex, Double, Double) => Complex,
      liquidityCf: (Complex) => Complex
  ): PortfolioMetrics = {
    val uDomain = FangOost.getUDomain(numU, xMin, 0.0)
    val logCf = getLogLpmCf(lgdCf, liquidityCf)
    val logLpmCFUDF =
      udf((balance: Double, pd: Double, lgd: Double, lgdVariance: Double) =>
        uDomain
          .map(u => logCf(u, balance, pd, lgd, lgdVariance))
          .map(ucf => {
            Seq(ucf.re, ucf.im)
          })
      )

    val cfUDF = udf(getCFForLoan _)
    val elUDF = udf(getElFromLoan _)
    val varUDF = udf(getVarFromLoan _)
    val lambdaUDF = udf(getLambdaFromLoan _)
    val complexSum = new SumArrayElement
    val doubleSum = new SumDoubleElement
    loanDF
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
        complexSum(col("cf")).alias("cf"),
        doubleSum(col("el")).alias("el"),
        doubleSum(col("variance")).alias("variance"),
        sum("lambda").alias("lambda")
      )
      .collect()
      .map({
        case Row(
              cf: Seq[Seq[Double]],
              el: Seq[Double],
              variance: Seq[Double],
              lambda: Double
            ) =>
          PortfolioMetrics(
            cf.map(v => Complex(v(0), v(1))),
            el,
            variance,
            lambda
          )
      })
      .head
  }
}
