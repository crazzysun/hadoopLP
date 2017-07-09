import java.util
import java.util.Properties

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, rdd, sql}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import org.apache.spark.sql._
import java.sql.{Connection, DriverManager, SQLException}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{row_number, count, max, broadcast}
import org.apache.spark.sql.expressions.Window



case class Sale(product: String, category: String, ip: String, date: String, price: Int)

object FlumePollingStreamClient {
  //  val logger = Logger(LoggerFactory.getLogger("FlumePollingStreamClient"))

  def main(argv: Array[String]): Unit = {
    val hostName = "127.0.0.1"
    var port = 54321
    val microBatchTime = 30

    if (argv.length == 1) {
      port = argv(0).toInt
    }
    listenFlume(hostName, port, microBatchTime)
  }

  def listenFlume(hostName: String, port: Int, microBatchTime: Int) {

    println(s"Listening on $hostName at $port batching records every $microBatchTime")

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")
      .set("spark.driver.port", "62966")
      .set("spark.blockManager.port", "62968")
      .set("spark.driver.blockManager.port", "62977")

    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(microBatchTime))

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkStreamingContext.sparkContext)
    val spark: org.apache.spark.sql.SparkSession = sqlContext.sparkSession

    import spark.implicits._

    val stream = FlumeUtils.createPollingStream(sparkStreamingContext, hostName, port)

    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    prop.put("driver", driver)

    val host = "localhost"
    val database = "test"

    def ex51(ds: Dataset[Sale]): Unit = {
      println("Write to db...")
      ds
        .groupBy("category").agg(count("*").alias("cnt")).sort($"cnt".desc).limit(10)
        .write
        .mode(SaveMode.Overwrite)
        .jdbc(s"jdbc:mysql://${host}:3306/${database}", "ex51", prop)
    }
    /*
    *
SELECT category, product, cnt, rank
FROM (
    SELECT category, product, cnt, row_number()
           over (PARTITION BY category ORDER BY cnt DESC) as rank
    FROM (
        SELECT category, product, count(product) AS cnt
        FROM test2
        GROUP BY category, product
    ) a
) ranked_mytable
WHERE ranked_mytable.rank <= 10
ORDER BY category, rank;
    *
    * */


    def ex52(ds: Dataset[Sale]): Unit = {
      def w = Window
        .partitionBy("category")
        .orderBy($"cnt".desc)

      val res = ds
        .groupBy("category", "product")
        .agg(count("product").alias("cnt"))
        .sort($"category".asc, $"cnt".desc)
//        .collect()
//        .agg(count("product").as("cnt1"))
        .withColumn("rank", row_number().over(w))
        .select("category", "product", "cnt", "rank")
        .where($"rank" <= 5)
//        .collect()
//        .map(row => println(row))
        .write
        .mode(SaveMode.Overwrite)
        .jdbc(s"jdbc:mysql://${host}:3306/${database}", "ex52", prop)


      ds
        .groupBy("category", "product")
        .agg(count("product").alias("cnt"))
        .sort($"category".asc, $"cnt".desc)
        //        .collect()
        //        .agg(count("product").as("cnt1"))
        .withColumn("rank", row_number().over(w))
        .select("category", "product", "cnt", "rank")
        //        .collect()
        //        .map(row => println(row))
        .write
        .mode(SaveMode.Overwrite)
        .jdbc(s"jdbc:mysql://${host}:3306/${database}", "ex53", prop)

//      println("AAAAAAAA")
//      println(res.toJSON.toString())
//      res.show()
    }

    val mappedlines = stream.map { sparkFlumeEvent =>
      val event = sparkFlumeEvent.event
      println("### Value of event " + event)

      val messageBody = new String(event.getBody.array())
      val chunks: Array[String] = messageBody.split(",")
      val sale = Sale(chunks(0), chunks(1), chunks(2), chunks(3), chunks(4).toInt)
      println("### Sale: " + sale)
      sale
    }.window(Seconds(30))
      .foreachRDD { rdd =>
        val c = rdd.count()
        println(s"RDD:${rdd} ${c}")
        if (c > 0) {
          val ds = rdd.toDS()
          ex52(ds)
        }
      }


    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()

    println("Exiting FlumePollingStreamClient.main")
  }


  def testDB() {
    val host = "localhost"
    val database = "demodb"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    println("loading driver...")

    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)

    val saleDS = ??? // Seq(Sale("cat1", "name1")).toDS()

    // val dataWrite = saleDS.select("cat1", "name1")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.put("driver", driver)

    println("write...")

    //saleDS.write.jdbc(s"jdbc:mysql://${host}:3306/${database}", "my_new_table", prop)


    spark.stop()
  }
}
