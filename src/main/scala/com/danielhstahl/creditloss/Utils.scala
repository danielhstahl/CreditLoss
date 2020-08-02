package com.danielhstahl.creditloss
import org.apache.spark.sql.types.{
  StructType,
  ArrayType,
  DoubleType,
  StructField,
  LongType,
  DataType
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}

object VecToMat {
  def getColFromIndex(index: Int, numRow: Int): Int = {
    index / numRow
  }
  def getRowFromIndex(index: Int, numRow: Int): Int = {
    index % numRow
  }
}

class SumArrayElement extends UserDefinedAggregateFunction {

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("value", ArrayType(ArrayType(DoubleType, true), true)) :: Nil
    )

  // Define how the aggregates types will be
  override def bufferSchema: StructType =
    StructType(
      StructField(
        "cumArray",
        ArrayType(ArrayType(DoubleType, true), true)
      ) :: Nil
    )

  // define the return type
  override def dataType: DataType = ArrayType(ArrayType(DoubleType, true), true)

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq(Seq(0.0, 0.0))
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = input
      .getAs[Seq[Seq[Double]]](0)
      .zipAll(
        buffer
          .getAs[Seq[Seq[Double]]](0),
        Seq(0.0, 0.0),
        Seq(0.0, 0.0)
      )
      .map { case (a, b) => Seq(a(0) + b(0), a(1) + b(1)) }

  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1
      .getAs[Seq[Seq[Double]]](0)
      .zipAll(
        buffer2
          .getAs[Seq[Seq[Double]]](0),
        Seq(0.0, 0.0),
        Seq(0.0, 0.0)
      )
      .map { case (a, b) => Seq(a(0) + b(0), a(1) + b(1)) }
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getSeq(0)
  }
}

class SumDoubleElement extends UserDefinedAggregateFunction {

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("value", ArrayType(DoubleType, true)) :: Nil
    )

  // Define how the aggregates types will be
  override def bufferSchema: StructType =
    StructType(
      StructField(
        "cumArray",
        ArrayType(DoubleType, true)
      ) :: Nil
    )

  // define the return type
  override def dataType: DataType = ArrayType(DoubleType, true)

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq(0.0)
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer
      .getAs[Seq[Double]](0)
      .zipAll(input.getAs[Seq[Double]](0), 0.0, 0.0)
      .map { case (a, b) => a + b }
    //buffer(1) = buffer.getAs[Double](1) + (1.toDouble / input.getAs[Double](0))
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1
      .getAs[Seq[Double]](0)
      .zipAll(buffer2.getAs[Seq[Double]](0), 0.0, 0.0)
      .map { case (a, b) => a + b }
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getSeq(0)
  }
}
