import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by KDF5000 on 16/10/1.
 */
public class CartesianDemo {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Spark Exercise");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data1 = sc.textFile("/user/hadoop/data.dat");
        JavaRDD<String> data2 = sc.textFile("/user/hadoop/data.dat");

        JavaPairRDD<String, String> res = data1.cartesian(data2);
        JavaPairRDD<String, String> cateRes = res.filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if(stringStringTuple2._1().contains("80")){
                    return true;
                }
                return false;
            }
        });
        cateRes.saveAsTextFile("hdfs:///user/hadoop/res.dat");

//        List<Tuple2<String, String>> top = res;
//        for( Tuple2<String, String> tuple : top){
//            System.out.println("Result: "+ tuple.toString());
//        }
    }
}
