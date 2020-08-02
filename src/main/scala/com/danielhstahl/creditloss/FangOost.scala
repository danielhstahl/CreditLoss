package com.danielhstahl.creditloss
import scala.math.Pi
import breeze.math.Complex
import scala.math
import breeze.optimize.RootFinding
object FangOost {
  private[this] def getU(du: Double, index: Int): Double = {
    du * index
  }
  def computeDu(xMin: Double, xMax: Double): Double = {
    Pi / (xMax - xMin)
  }
  def getUDomain(numU: Int, xMin: Double, xMax: Double): Seq[Complex] = {
    val du = computeDu(xMin, xMax)
    (1 to numU).map(index => Complex(0.0, getU(du, index - 1)))
  }
  def getDiscreteCf(
      numU: Int,
      xMin: Double,
      xMax: Double,
      cfFn: (Complex) => Complex
  ): Seq[Complex] = {
    getUDomain(numU, xMin, xMax).map(cfFn)
  }
  private[this] def computeDx(
      xDiscrete: Int,
      xMin: Double,
      xMax: Double
  ): Double = {
    (xMax - xMin) / (xDiscrete - 1)
  }
  private[this] def getX(xMin: Double, dx: Double, index: Int): Double = {
    xMin + index * dx
  }
  def getXDomain(xDiscrete: Int, xMin: Double, xMax: Double): Seq[Double] = {
    val dx = computeDx(xDiscrete, xMin, xMax)
    (1 to xDiscrete).map(i => getX(xMin, dx, i - 1))
  }
  private[this] def computeCp(du: Double): Double = {
    (2.0 * du) / Pi
  }
  private[this] def convoluteVk(
      cfIncr: Complex,
      x: Double,
      uIm: Double,
      uIndex: Int,
      vk: (Double, Double, Int) => Double
  ): Double = {
    cfIncr.re * vk(uIm, x, uIndex)
  }
  private[this] def adjustIndex(index: Int): Double = {
    if (index == 0) 0.5 else 1.0
  }
  private[this] def integrateCf(
      cfAdjusted: Seq[Complex],
      du: Double,
      x: Double,
      convolute: (Complex, Double, Double, Int) => Double
  ) = {
    cfAdjusted.zipWithIndex
      .map({
        case (cf, i) => convolute(cf * adjustIndex(i), x, getU(du, i), i)
      })
      .sum
  }
  private[this] def adjustCf(
      cfInc: Complex,
      u: Complex,
      xMin: Double,
      cp: Double
  ): Complex = {
    cfInc * (-u * xMin).exp * cp
  }
  private[this] def getDiscreteCfAdjusted(
      xMin: Double,
      xMax: Double,
      cf: Seq[Complex]
  ): Seq[Complex] = {
    val du = computeDu(xMin, xMax)
    val cp = computeCp(du)
    getUDomain(cf.length, xMin, xMax)
      .zip(cf)
      .map({ case (u, f) => adjustCf(f, u, xMin, cp) })
  }
  private[this] def getExpectationGenericSingleElement(
      xMin: Double,
      xMax: Double,
      x: Double,
      cf: Seq[Complex],
      convolute: (Complex, Double, Double, Int) => Double
  ): Double = {
    val du = computeDu(xMin, xMax)
    integrateCf(getDiscreteCfAdjusted(xMin, xMax, cf), du, x, convolute)
  }
  private[this] def getExpectationGeneric(
      xMin: Double,
      xMax: Double,
      xDomain: Seq[Double],
      cf: Seq[Complex],
      convolute: (Complex, Double, Double, Int) => Double
  ): Seq[Double] = {
    val du = computeDu(xMin, xMax)
    val discreteCfAdjusted = getDiscreteCfAdjusted(xMin, xMax, cf)
    xDomain.map(x => integrateCf(discreteCfAdjusted, du, x, convolute))
  }
  def getExtendedExpectation(
      xMin: Double,
      xMax: Double,
      xDomain: Seq[Double],
      cf: Seq[Complex],
      vk: (Double, Double, Int) => Double
  ) = {
    getExpectationGeneric(
      xMin,
      xMax,
      xDomain,
      cf,
      (cf, x, u, i) => convoluteVk(cf, x, u, i, vk)
    )
  }
  def getExtendedExpectationSingleElement(
      xMin: Double,
      xMax: Double,
      x: Double,
      cf: Seq[Complex],
      vk: (Double, Double, Int) => Double
  ) = {
    getExpectationGenericSingleElement(
      xMin,
      xMax,
      x,
      cf,
      (cf, x, u, i) => convoluteVk(cf, x, u, i, vk)
    )
  }
  def getDensity(
      xMin: Double,
      xMax: Double,
      xDomain: Seq[Double],
      cf: Seq[Complex]
  ): Seq[Double] = {
    getExtendedExpectation(
      xMin,
      xMax,
      xDomain,
      cf,
      (u, x, i) => math.cos(u * (x - xMin))
    )
  }
  private[this] def vkCdf(u: Double, x: Double, a: Double, k: Int): Double = {
    if (k == 0) x - a else math.sin((x - a) * u) / u
  }
  private[this] def diffPow(x: Double, a: Double): Double = {
    0.5 * (x * x - a * a)
  }
  private[this] def vkPe(u: Double, x: Double, a: Double, k: Int): Double = {
    val arg = (x - a) * u
    val uDen = 1.0 / u
    if (k == 0) diffPow(x, a)
    else x * math.sin(arg) * uDen + uDen * uDen * (math.cos(arg) - 1.0)
  }
  private[this] def getValueAtRisk(
      alpha: Double,
      xMin: Double,
      xMax: Double,
      cf: Seq[Complex]
  ): Double = {
    val f = (x: Double) =>
      getExtendedExpectationSingleElement(
        xMin,
        xMax,
        x,
        cf,
        (u, x, uIndex) => vkCdf(u, x, xMin, uIndex)
      ) - alpha
    -RootFinding.find(f, xMin, Some(xMax))
  }
  private[this] def getExpectedShortfall(
      alpha: Double,
      xMin: Double,
      xMax: Double,
      valueAtRisk: Double,
      cf: Seq[Complex]
  ): Double = {
    -getExtendedExpectationSingleElement(
      xMin,
      xMax,
      -valueAtRisk,
      cf,
      (u, x, uIndex) => vkPe(u, x, xMin, uIndex)
    )/alpha
  }

  def getRiskMetrics(
      alpha: Double,
      xMin: Double,
      xMax: Double,
      cf: Seq[Complex]
  ): (Double, Double) = {
    val valueAtRisk = getValueAtRisk(alpha, xMin, xMax, cf)

    val expectedShortfall =
      getExpectedShortfall(alpha, xMin, xMax, valueAtRisk, cf)
    (valueAtRisk, expectedShortfall)
  }
  def getExpectation(xMin: Double, xMax: Double, cf: Seq[Complex]): Double = {
    getExtendedExpectationSingleElement(
      xMin,
      xMax,
      xMax,
      cf,
      (u, x, uIndex) => vkPe(u, x, xMin, uIndex)
    )
  }
}
