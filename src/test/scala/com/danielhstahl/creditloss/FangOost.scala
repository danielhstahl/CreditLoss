package com.danielhstahl.creditloss
import breeze.math.Complex
import org.scalatest._
import scala.math.Pi
/*class RiskMetrics extends FunSuite {
  test("it computes VaR correctly") {
    val mu = 2.0
    val sigma = 5.0
    val numU = 128
    val xMin = (-20.0)
    val xMax = 20.0
    val alpha = 0.05
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * mu * mu).exp
    val referenceVar = 6.224268
    val referenceEs = 8.313564
    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    val (actualVar, actualEs) =
      FangOost.getRiskMetrics(alpha, xMin, xMax, discreteCf)
    assert(actualVar === referenceVar)
    assert(actualEs === referenceEs)
  }
}*/
//sbt -java-home /usr/lib/jvm/java-8-openjdk-amd64 test

class ComputeInv extends FunSuite {
  test("It computes inverse") {
    val mu = 2.0
    val sigma = 1.0
    val numU = 256
    val xMin = (-3.0)
    val xMax = 7.0
    val numX = 5
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * mu * mu).exp
    val xDomain = FangOost.getXDomain(numX, xMin, xMax)
    val refNormal = xDomain.map(x =>
      (math.exp(math.pow(x - mu, 2) / (2.0 * sigma * sigma)) / (sigma * math
        .sqrt(Pi * 2.0)))
    )
    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    val myInverse = FangOost.getDensity(xMin, xMax, xDomain, discreteCf)

    for ((ref, est) <- refNormal.zip(myInverse)) {
      assert(math.abs(ref - est) < 0.0001)
    }
  }
}

class Domains extends FunSuite {
  test("x domain") {
    val result = FangOost.getXDomain(5, 0.0, 1.0)
    assert(result(0) === 0.0)
    assert(result(1) === 0.25)
    assert(result(2) === 0.5)
    assert(result(3) === 0.75)
    assert(result(4) === 1.0)
  }
  /*test("u domain") {
    val result = FangOost.getUDomain(5, 0.0, 1.0)
    assert(result(0) === 0.0)
    assert(result(1) === 0.25)
    assert(result(2) === 0.5)
    assert(result(3) === 0.75)
    assert(result(4) === 1.0)
  }*/
}
