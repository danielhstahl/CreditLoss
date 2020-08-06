| [Linux][lin-link] |  
| :---------------: | 
| ![lin-badge]      | 

[lin-badge]: https://github.com/phillyfan1138/CreditLoss/workflows/test/badge.svg
[lin-link]:  https://github.com/phillyfan1138/CreditLoss/actions

## Spark Loan Loss Distribution

This library is a Spark implementation of my [Credit Risk ](https://github.com/phillyfan1138/CreditRiskPaper) model.

It is assumed that you have a dataframe with the following fields:

* balance: double 
* lgd: the "expected" loss given default, a positive number between 0 and 1
* pd: the "expected" probability of default
* lgd_variance: the variance around the expected lgd for each loan
* r: the fraction of the balance that is expected to be lost in the case of a liquidity event.  Can be set to zero if liquidity is not a concern.
* weight: an array of weights that represent the exposure of the loan to each systemic variable.  Must sum to 1.
* num: number of loans that have these characteristics.  If done at the portfolio level, this can be set to 1.  If grouped across characteristics (eg, if pd, lgd, and balance are "bucketed"), then this may be larger than 1.  Grouping will provide speed improvements.

The LoanLoss class creates the discrete characteristic function and computes portfolio metrics such as expected loss and variance. 

The LgdCF and SystemicVariableCF objects contain example characteristic functions for each loan's loss given default distribution and the systemic variables' distributions, respectively.  You can, of course, use custom functions instead.

The FangOost object converts the discrete characteristic function into metrics such as expected loss and variance (not strictly needed since these are analytically provided through the LoanLoss class, but can provide a check for the accuracy of the numerical integration), value at risk and expected shortfall, and a graph of the density.  


```scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import com.danielhstahl.creditloss.{
    LoanLossSim, FangOost, LoanLoss, LgdCF, SystemicVariableCF
}
val spark = SparkSession.builder.config().getOrCreate()
val numLoans = 10000
val seed = 42
//In real use cases, this would be your loan portfolio
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

//steps in the complex plain.  
//The higher, the more accurate the algorithm, but also slower
val numDiscreteU = 256 

//this is the primary function to run the model.  
//It is computationally intensive.
val metrics = ll.getPortfolioMetrics(
    loanDF, 
    numDiscreteU,
    //the characteristic function of the loss given default distribution, 
    //scaled by the potential loss.  In this case, there is no 
    //"distribution"; its simply a constant
    LgdCF.degenerateCf 
)
//get the discrete characteristic function of the full distribution
val cf = ll.getFullCF(
    metrics.cf, 
    (e: Seq[Double], v: Seq[Double]) => SystemicVariableCF.gammaMgf(v))
)

val alpha=0.05 // 95% value at risk
val (valueAtRisk, expectedShortfall) = FangOost.getRiskMetrics(
    alpha, metrics.xMin, 0.0, cf
)

```