package com.danielhstahl.creditloss
import scala.math.Pi
import breeze.math.Complex
object FangOost {
  private[this] def getComplexU(u: Double): Complex = {
    Complex(0.0, u)
  }
  private[this] def getU(du: Double, index: Int): Double = {
    du * index
  }
  def computeDu(xMin: Double, xMax: Double): Double = {
    Pi / (xMax - xMin)
  }
  def getUDomain(numU: Int, xMin: Double, xMax: Double): Seq[Complex] = {
    val du = computeDu(xMin, xMax)
    (1 to numU).map(index => getComplexU(getU(du, index - 1)))
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
    2.0 * du / Pi
  }
  private[this] def convolute(
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
      discreteCfAdjusted: Seq[Complex],
      du: Double,
      x: Double,
      convolute: (Complex, Double, Double, Int) => Double
  ) = {
    discreteCfAdjusted.view.zipWithIndex
      .map({
        case (cf, i) => convolute(cf * adjustIndex(i), x, getU(du, i), i)
      })
      .sum
  }
  private[this] def adjustCf(
      fnInvIncrement: Complex,
      u: Complex,
      xMin: Double,
      cp: Double
  ): Complex = {
    fnInvIncrement * (-u * xMin).exp * cp
  }
  private[this] def getDiscreteCfAdjusted(
      xMin: Double,
      xMax: Double,
      fnInv: Seq[Complex]
  ): Seq[Complex] = {
    val du = computeDu(xMin, xMax)
    val cp = computeCp(du)
    getUDomain(fnInv.length, xMin, xMax)
      .zip(fnInv)
      .map({ case (u, f) => adjustCf(f, u, xMin, cp) })
      .toSeq
  }
  private[this] def getExpectationGenericSingleElement(
      xMin: Double,
      xMax: Double,
      x: Double,
      fnInv: Seq[Complex],
      convolute: (Complex, Double, Double, Int) => Double
  ): Double = {
    val du = computeDu(xMin, xMax)
    integrateCf(getDiscreteCfAdjusted(xMin, xMax, fnInv), du, x, convolute)
  }
}
