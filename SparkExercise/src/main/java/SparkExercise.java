import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by KDF5000 on 16/9/28.
 */

class Sum implements Function2<Integer, Integer, Integer> {

    public Integer call(Integer a, Integer b) throws Exception {
        return a+b;
    }
}

public class SparkExercise {


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Spark Exercise");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        List<Integer> data = Arrays.asList(1,2,3,4,5,6);
//        JavaRDD<String> collectionData = sc.parallelize(data);

        JavaRDD<String> data = sc.textFile("/user/hadoop/README.md");

        JavaRDD<Integer> linesLength = data.map(new Function<String, Integer>(){

            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int totalLength = linesLength.reduce(new Sum());

        System.out.println("Result: "+totalLength);
    }

}
