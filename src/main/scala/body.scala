import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession
object body{

  def main(args: Array[String]): Unit = {
    println("Hello, world!")


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val segments = spark.read.option("sep", "\t")
      .option("header", "true")
      .csv("/Users/moradigd/Documents/NLP/output.txt")
      .toDF()

    segments.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people WHERE Label = 'N'")
    sqlDF.show()



    val segments_1 = segments.select("sequence")

    println(segments_1.count())
    println(segments_1.columns.size)
    segments_1.printSchema()


    val tokenizer = new Tokenizer()
      .setInputCol("sequence")
      .setOutputCol("sequence_words")

    println(segments_1.first())

    val newDataSet = tokenizer.transform(segments_1)

    println(newDataSet.first())
    println(newDataSet.first())
  }

  def add(a:Int, b:Int): Int={
    return a+b
  }
}
