import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.spark.sql.catalyst.expressions.WindowFunctionType;
import org.apache.spark.sql.catalyst.plans.Cross;
import org.json4s.JsonAST;
import scala.Serializable;
import scala.Tuple2;

class Main {


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        //Config spark
        SparkConf conf = new SparkConf().setAppName("Dataframes").setMaster("local");
        //Start spark conext
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession sparkS = SparkSession
                .builder()
                .appName("test")
                .config("spark.some.config.option","some-value")
                .getOrCreate();



        Dataset<Row> small9DF = sparkS.read().format("json").load("C:\\Users\\Alexander Eric\\Desktop\\Skola augsburg\\JavaSpark\\cluster\\files\\wiki_small_9_100lines.json");
        small9DF.printSchema();
        Dataset<Row> ts = small9DF.select(small9DF.col("id").as("title_id"),
                org.apache.spark.sql.functions.explode(small9DF.col("revision.contributor")).as("contributors"));
        ts.printSchema();
        ts.show(20,false);
        Dataset<Row> contributorsDF = ts.select(ts.col("title_ID").as("title_id"),ts.col("contributors.id").as("con_id"));
        contributorsDF.printSchema();
        contributorsDF.show(20,false);


        Dataset<Row> contributorsDF1 = contributorsDF.select(
                contributorsDF.col("title_id").as("title_id1"),
                contributorsDF.col("con_id").as("con_id1"));
        contributorsDF1.show(20,false);

        Dataset<Row> conJoined = contributorsDF.crossJoin(contributorsDF1).filter("con_id != con_id1");
        Dataset<Row> task4 = conJoined.select("title_id","con_id","title_id1","con_id1")
                .groupBy(contributorsDF.col("con_id"), contributorsDF1.col("con_id1")).count().as("sumSimilair");
        //Count authorpairings, count similar articles for each author pairing, count disimilar articles for each author pairing. select
        conJoined.printSchema();
        conJoined.show(20);

        //Dataset<Row> pairCont = conJoined.withColumn(); //SQL for counting pairs (distinct).
        // con_id != con_id1, con_id
     }
}