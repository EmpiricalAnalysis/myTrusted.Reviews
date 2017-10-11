import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.Encoders
import scala.reflect.runtime.universe._
import org.apache.spark.sql.types._
import scala.util.Random.nextFloat
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext    
import org.apache.spark.SparkContext._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._



object  writeAllReviews2Es {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WriteAllReviews2Es").setMaster("spark://"+sys.env("SPARK_MASTER_IP")+":7077")

    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes.discovery", "true")
    conf.set("es.nodes", sys.env("ES_NODE1_IP"))
    conf.set("es.port", "9200")
    conf.set("es.nodes.discovery","ture")
    conf.set("es.nodes.client.only","false")
    conf.set("es.nodes.wan.only","false")
    conf.set("es.net.http.auth.user",sys.env("ES_USR"))
    conf.set("es.net.http.auth.pass",sys.env("ES_PWD"))
    conf.set("es.resource", "elasticsearch/spark")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val reviewsFile = spark.read.json(sys.env("REVIEWS_PATH")+"/*")
    val reviewsFile2 = reviewsFile
                      .select(col("reviewerID"),col("reviewText"),col("summary"),col("asin"),col("overall").alias("rating"),col("unixReviewTime"), col("helpful").getItem(0).alias("NEndorse"))
                      .withColumn("reviewID",monotonicallyIncreasingId)

    val review_table=reviewsFile2.select("reviewID", "reviewText", "summary", "rating", "unixReviewTime", "asin")

    review_table.saveToEs("reviews3/content")  

    }

}


