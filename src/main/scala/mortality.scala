import org.apache.spark._
import org.apache.spark.sql._

object mortality extends App {

  val spark = SparkSession
      .builder()
      .appName("Mortality Data")
      .master("local")
      .getOrCreate()

  spark.sql("SELECT * FROM events WHERE EXISTS c")

  val events = spark.read.csv("./events.csv")
  val mrt = spark.read.csv("./mortality.csv")

  val aliveDF = spark.sql("SELECT * FROM " + events)

  println(aliveDF.take(10))


}
