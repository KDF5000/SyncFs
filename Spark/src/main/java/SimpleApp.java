import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by KDF5000 on 16/9/27.
 */
public class SimpleApp{

    public void run() {
//        super.run();
        String logFile = "/user/hadoop/README.md";
        SparkConf conf = new SparkConf().setAppName("Java Simple App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile);

        long numAs = logData.filter(new Function<String, Boolean>() {

            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {

            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }

    public static void main(String []args){
        SimpleApp simpleApp = new SimpleApp();
        simpleApp.run();
    }

}
