import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by KDF5000 on 16/9/28.
 */
public class SparkEntry implements Serializable {

    protected SparkConf conf = null;
    protected JavaSparkContext sc = null;

    protected void init(){
        System.out.println("init");
        this.conf = new SparkConf().setAppName("Java Simple App");
        this.sc = new JavaSparkContext(conf);
    }
    public SparkEntry(){
        init();
    }

    //need to be override
    public void run(){};

}
