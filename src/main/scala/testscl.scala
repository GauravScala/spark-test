


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions;
import org.apache.spark.sql._;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.functions.col;



case class namecnt(name : String,count :Int)

object testscl {
    def main(args: Array[String]) = {
      

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
import sqlcontext.implicits._;    

    //Read some example file to a test RDD
    val test = sc.textFile("/Users/g0k004g/gau6.txt")
    test.foreach(println);
    //test.flatMap(x => x.split(",")).map(x => (x,1)).saveAsTextFile("file:///Users/g0k004g/gauscala2")
    //test.flatMap(x => x.split(",")).map(x => (x,1)).reduceByKey(_ + _).saveAsTextFile("file:///Users/g0k004g/gauscalar")
    var test1 = test.flatMap(x => x.split(",")).map(x => (x,1))
    test1.foreach(println);
    
    //var testmap = test1.map(x => namecnt(x._1.toString(),x._2.toInt)).toDF()
    var testdf = test1.map(x => namecnt(x._1,x._2)).toDF()
    testdf.show()
    testdf.registerTempTable("sample")
    var outsql = sqlcontext.sql("select count(*) from sample")
    outsql.show()
    testdf.select("*").show()
    testdf.groupBy("name","count").agg(sum("count"),avg("count")).show()
   // var wnd = Window.partitionBy("name").orderBy("count").rangeBetween(-1, 1)
    var wnd = Window.orderBy(col("name").desc)
    
    var leadex = lead("name",1).over(wnd)
    var df2 = testdf.withColumn("lead", leadex)
    df2.show()
    
    
    }
  
  
  
}