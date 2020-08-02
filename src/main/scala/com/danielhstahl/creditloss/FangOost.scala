package com.danielhstahl.creditloss
import scala.math.Pi
import breeze.math.Complex
object FangOost {
  def getComplexU(u: Double): Complex = {
    Complex(0.0, u)
  }
  def getU(du: Double, index: Int): Double = {
    du * index
  }
  def computeDu(xMin: Double, xMax: Double): Double = {
    Pi / (xMax - xMin)
  }
  def getUDomain(numU: Int, xMin: Double, xMax: Double): Seq[Complex] = {
    val du = computeDu(xMin, xMax)
    (1 to numU).map(index => getComplexU(getU(du, index - 1)))
  }
}
