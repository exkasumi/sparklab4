import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import vegas._
import vegas.sparkExt.VegasSpark

object SparkDatasetAnalysis {
  def main(args: Array[String]): Unit = {

    // Создаем SparkContext
    val NODES = 4
    val spark = SparkSession.builder.appName("Analyze Dataset").master(s"local[$NODES]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val schema = new StructType()
      .add("name", StringType, true)
      .add("department", StringType, true)
      .add("job", StringType, true)
      .add("date_started", StringType, true)
      .add("current_position", StringType, true)
      .add("date_term", StringType, true)
      .add("sex", StringType, true)
      .add("status", StringType, true)
      .add("race", StringType, true)
      .add("regular_pay", DoubleType,true)
      .add("premium_pay", DoubleType,true)
      .add("other_pay", DoubleType,true)
      .add("total_pay", DoubleType,true)

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df_load = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("C:\\bigdata\\salary2018.csv")

    var df = df_load.select(col("department"), col("sex"),
      col("race"), col("total_pay"))
    df = df.na.drop

    val ind1 = new StringIndexer()
      .setInputCol("department")
      .setOutputCol("dep")
    df = ind1.fit(df).transform(df)

    val ind2 = new StringIndexer()
      .setInputCol("sex")
      .setOutputCol("s")
    df = ind2.fit(df).transform(df)

    val ind3 = new StringIndexer()
      .setInputCol("race")
      .setOutputCol("rac")
    df = ind3.fit(df).transform(df)

    df = df.drop(col("department"))
    df = df.drop(col("sex"))
    df = df.drop(col("race"))

    val assembler = new VectorAssembler()
      .setInputCols(Array("total_pay", "s", "rac"))
      .setOutputCol("features")
    df = assembler.transform(df)

    val Array(training_data, test_data) = df.randomSplit(Array(.7, .3))
    println("Number of all rows:", df.count())
    println("Number of training rows:", training_data.count())
    println("Number of test rows:", test_data.count())

    val rf = new RandomForestClassifier()
      .setLabelCol("rac")
      .setFeaturesCol("features")
      .setNumTrees(11)
      .setMaxBins(11)

    val model = rf.fit(training_data)
    val predictions = model.transform(test_data)
    predictions.show(15, truncate = false)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("rac").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Accuracy = $accuracy")

    Vegas().
      withDataFrame(predictions).
      encodeX("rac", Ordinal).
      encodeY(field = "total_pay", Quantitative, aggregate = AggOps.Average).
      mark(Bar).
      show
    Vegas().
      withDataFrame(predictions).
      encodeX("prediction", Ordinal).
      encodeY(field = "total_pay", Quantitative, aggregate = AggOps.Average).
      mark(Bar).
      show

  }
}