package com.danielhstahl.creditloss
import org.apache.spark.sql.{SparkSession, DataFrame, Row, SQLContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, ArrayType, DoubleType, StringType, StructField}


import scala.util.Random


object LoanLoss {
    val schema=StructType(
        StructField("balance", DoubleType, false),
        StructField("pd", DoubleType, false),
        StructField("lgd", DoubleType, false),
        StructField("weight", ArrayType(DoubleType, false), false),
        StructField("r", DoubleType, true),
        StructField("lgd_variance", DoubleType, true),
    )
    val balanceRange=(1000.0, 1000000.0)
    val pdCategories=List(0.0001, 0.0002, 0.0005, .001, .003, .006, 0.01, 0.04, 0.1)
    val lgdCategories=List(0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1)
    val rCategories=List(0.0, 0.2, 0.4)
    val lgdVarianceCategories=List(0.1, 0.2, 0.3, 0.5)
    val weightCategories=List(
        Array(0.3, 0.5, 0.2), Array(0.2, 0.2, 0.6), Array(0.1, 0.6, 0.3), Array(0.4, 0.2, 0.4)
    )
    def simulateRange(min: Double, max: Double, r: Random):Double={
        min+(max-min)*r.nextDouble
    }
    def simulateCategory[A](categories:List[A], r:Random):A={
        categories[r.nextInt(categories.length)]
    }
    def simulatePortfolio(spark:SparkSession,numLoans:Int, seed:Int):DataFrame={
        val r=new Random(seed)
        val rows=(1 to numSims).map(v=>simulateLoan(r))
        spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }
    def simulateLoan(r:Random):Row={
        Row(
            simulateRange(balanceRange._1, balanceRange._2, r),
            simulateCategory(pdCategories, r),
            simulateCategory(lgdCategories, r),
            simulateCategory(weightCategories, r),
            simulateCategory(rCategories, r),
            simulateCategory(lgdVarianceCategories, r)
        )
    }
}