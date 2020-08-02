package com.danielhstahl.creditloss
import breeze.math.Complex
import org.scalatest._
class RiskMetrics extends FunSuite {
  test("it computes VaR correctly") {
    val mu=2.0
    val sigma=5.0
    val numU=128
    val xMin=-20.0
    val xMax=20.0
    val alpha=0.05
    val normCf=(u:Complex)=>(u*mu+0.5*sigma*sigma*mu*mu).exp
    val referenceVar = 6.224268
    val referenceEs = 8.313564
    val discreteCf=getDiscreteCf(numU, xMin, xMax, normCf)
    val (actualVar, actualEs)=getRiskMetrics(alpha, xMin, xMax, discreteCf)
    assert(actualVar === referenceVar)
    assert(actualEs===referenceEs)
  }
}
