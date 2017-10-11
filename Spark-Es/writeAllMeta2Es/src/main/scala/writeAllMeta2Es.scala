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



object  writeAllMeta2Es {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WriteAllMeta2Es").setMaster("spark://"+sys.env("SPARK_MASTER_IP")+":7077")

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
    val reviews3 = reviewsFile.select(col("reviewerID"), col("asin")).withColumn("reviewID",monotonicallyIncreasingId)
    val metaFile1 = spark.read.json(sys.env("META_PATH")+"/*")
    val meta=metaFile1.withColumn("category", col("categories").getItem(0).getItem(0).alias("categories")).drop("categories")

    var newColumnNames = Seq[String]()

    for( a <- meta.columns ){
         newColumnNames = newColumnNames :+ a;
    }

    val meta6=meta.toDF(newColumnNames: _*)
    val meta_review = reviews3.join(meta6, reviews3("asin") === meta6("asin")))
    val uber_meta = meta_review.distinct.cache
    uber_meta.saveToEs("uber_meta/content")


    }

}


