package com.danielhstahl.creditloss
import breeze.math.Complex

object SystemicVariableCF {
  def gammaMgf(variance: Seq[Double]): (Seq[Complex]) => Complex = {
    (uWeights: Seq[Complex]) =>
      {
        uWeights
          .zip(variance)
          .map({ case (u, v) => (-(1.0 - v * u).log / v) })
          .sum
          .exp
      }
  }
  def degenerateMgf(uWeights: Seq[Complex]): Complex = {
    uWeights.sum.exp
  }
}

object LgdCF {
  def degenerateCf(u: Complex, l: Double, lgdVariance: Double): Complex = {
    (-u * l).exp //negative since l is a loss
  }
  def gammaCf(u: Complex, l: Double, lgdVariance: Double): Complex = {
    (1.0 + u * l * lgdVariance).pow(-1.0 / lgdVariance)
  }
}
