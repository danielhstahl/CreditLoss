| [Linux][lin-link] |  
| :---------------: | 
| ![lin-badge]      | 

[lin-badge]: https://github.com/phillyfan1138/CreditLoss/workflows/test/badge.svg
[lin-link]:  https://github.com/phillyfan1138/CreditLoss/actions

## Spark Loan Loss Distribution

This library is a Spark implementation of my [Credit Risk ](https://github.com/phillyfan1138/CreditRiskPaper) model.

```scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import com.danielhstahl.creditloss.{
    LoanLossSim, FangOost, LoanLoss, LgdCF, SystemicVariableCF
}
val spark = SparkSession.builder.config().getOrCreate()
val numLoans = 10000
val seed = 42
/** In real use cases, this wouldn't be needed...but its handy for testing */
val loanDF: DataFrame = LoanLossSim.simulatePortfolio(spark, numLoans, seed)

val q = 0.00001 // q*portfolio loss is the probability of liquidity event
val lambda = 50.0 // base loss in the case of a liquidity event
val systemicExpectation = Seq(1.0, 1.0, 1.0) //3 systemic variables
val systemicVariance = Seq(0.3, 0.3, 0.3)
val ll = new LoanLoss(
    q,
    lambda,
    systemicExpectation,
    systemicVariance    
)
val numDiscreteU = 256 //steps in the complex plain.  The higher, the more accurate the algorithm, but also slower

//this is the primary function to run the model.  It is computationally intensive.
val metrics = ll.getPortfolioMetrics(
    loanDF, 
    numDiscreteU,
    LgdCF.degenerateCf //the characteristic function of the loss given default distribution, scaled by the potential loss.  In this case, there is no "distribution"; its simply a constant
)
//get the discrete characteristic function of the full distribution
val cf = ll.getFullCF(
    metrics.cf, 
    (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v))
)

val alpha=0.05 // 95% value at risk
val (valueAtRisk, expectedShortfall) = FangOost.getRiskMetrics(
    alpha, ll.xMin, 0.0, cf
)

```