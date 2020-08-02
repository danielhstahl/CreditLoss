package com.danielhstahl.creditloss
import breeze.math.Complex
import org.scalatest._
import scala.math.Pi
import org.scalatest.PrivateMethodTester._
class RiskMetrics extends FunSuite {
  test("it computes VaR correctly") {
    val mu = 2.0
    val sigma = 5.0
    val numU = 128
    val xMin = (-20.0)
    val xMax = 20.0
    val alpha = 0.05
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * u * u).exp
    val referenceVar = 6.224268
    val referenceEs = 8.313564
    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    val (actualVar, actualEs) =
      FangOost.getRiskMetrics(alpha, xMin, xMax, discreteCf)
    assert(math.abs(actualVar -referenceVar)<0.0001)
    assert(math.abs(actualEs -referenceEs)<0.0001)
  }
  test("it computes expectation correctly") {
    val mu = 2.0
    val sigma = 5.0
    val numU = 128
    val xMin = (-20.0)
    val xMax = 20.0
    val alpha = 0.05
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * u * u).exp
    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    val actualEl=FangOost.getExpectation(xMin, xMax,discreteCf)
    assert(math.abs(mu-actualEl)<0.000001)

  }
}
//sbt -java-home /usr/lib/jvm/java-8-openjdk-amd64 test

class ComputeInv extends FunSuite {
  test("It creates discrete cf") {
    val mu = 2.0
    val sigma = 1.0
    val numU = 5
    val xMin = (-3.0)
    val xMax = 7.0
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * u * u).exp
    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    assert(
      discreteCf === Seq(
        Complex(1.0, 0.0),
        Complex(0.7700626702542623, 0.5594832791690904),
        Complex(0.2536623838321682, 0.7806925427208942),
        Complex(-0.19819751328298396, 0.6099892237401136),
        Complex(-0.3673266737688966, 0.2668784501638547)
      )
    )
  }
  test("It computes inverse") {
    val mu = 2.0
    val sigma = 1.0
    val numU = 256
    val xMin = (-3.0)
    val xMax = 7.0
    val numX = 5
    val normCf = (u: Complex) => (u * mu + 0.5 * sigma * sigma * u * u).exp
    val xDomain = FangOost.getXDomain(numX, xMin, xMax)
    val refNormal = xDomain.map(x =>
      (math.exp(-math.pow(x - mu, 2) / (2.0 * sigma * sigma)) / (sigma * math
        .sqrt(Pi * 2.0)))
    )

    val discreteCf = FangOost.getDiscreteCf(numU, xMin, xMax, normCf)
    val actualNormal = FangOost.getDensity(xMin, xMax, xDomain, discreteCf)
    for ((ref, est) <- refNormal.zip(actualNormal)) {
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
  test("u domain") {
    val result = FangOost.getUDomain(5, -1.0, 3.0)
    assert(result(0) === Complex(0.0, 0.0))
    assert(result(1) === Complex(0.0, 0.7853981633974483))
    assert(result(2) === Complex(0.0, 1.5707963267948966))
    assert(result(3) === Complex(0.0, 2.356194490192345))
    assert(result(4) === Complex(0.0, 3.141592653589793))
  }

}

class Helpers extends FunSuite {
  test("integrateCf with just index") {
    val integrateCf = PrivateMethod[Double]('integrateCf)
    val cfAdjusted = Seq(Complex(0.5, 0.5), Complex(1.0, 1.0))
    val du = 0.5
    val x = 1.0
    val convolute =
      (cf: Complex, x: Double, u: Double, index: Int) => index.toDouble
    val result =
      FangOost invokePrivate integrateCf(cfAdjusted, du, x, convolute)
    assert(result === 1.0) //0 + 1
  }
  test("integrateCf with just cf") {
    val integrateCf = PrivateMethod[Double]('integrateCf)
    val cfAdjusted = Seq(Complex(0.5, 0.5), Complex(1.0, 1.0))
    val du = 0.5
    val x = 1.0
    val convolute = (cf: Complex, x: Double, u: Double, index: Int) => cf.re
    val result =
      FangOost invokePrivate integrateCf(cfAdjusted, du, x, convolute)
    assert(result === 1.25) //0.25 + 1.0
  }
  test("getDiscreteCfAdjusted") {
    val xMin = (-1.5)
    val xMax = 5.0
    val cf = Seq(Complex(0.5, 0.5), Complex(1.0, 1.0))
    val getDiscreteCfAdjusted =
      PrivateMethod[Seq[Complex]]('getDiscreteCfAdjusted)
    val result =
      FangOost invokePrivate getDiscreteCfAdjusted(xMin, xMax, cf)
    assert(
      result === Seq(
        Complex(0.15384615384615383, 0.15384615384615383),
        Complex(0.026273258440094184, 0.43434874043442956)
      )
    )
  }
  test("getExpectationGenericSingleElement") {
    val xMin = (-1.5)
    val xMax = 5.0
    val x = 2.0
    val cf = Seq(Complex(0.5, 0.5), Complex(1.0, 1.0))
    val convolute = (cf: Complex, x: Double, u: Double, index: Int) => cf.re * x
    val getExpectationGenericSingleElement =
      PrivateMethod[Double]('getExpectationGenericSingleElement)
    val result =
      FangOost invokePrivate getExpectationGenericSingleElement(
        xMin,
        xMax,
        x,
        cf,
        convolute
      )
    assert(
      result === 0.2063926707263422
    )
  }
}
