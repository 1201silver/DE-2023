import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.text.*;
import java.util.*;

public final class UBERStudent20191022 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20191022 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20191022")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
            	
            	String[] line = s.split(",");
            	String region = line[0];
            	String day = line[1];
            	String vehicles = line[2];
            	String trips = line[3];
            
            	String d = "";
            	try {
            		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
            		Date date = df.parse(day);
            		DateFormat df2 = new SimpleDateFormat("EEE", Locale.ENGLISH);
            		d = (df2.format(date)).toUpperCase();
            		//System.out.println("--------------\n" +);
            		
            	} catch(ParseException e) {
            		e.printStackTrace();
            	}
            	
            	String uber = region + "," + d + "::" + trips + "," + vehicles;
            	
                return Arrays.asList(uber).iterator();
            }
        };
        JavaRDD<String> words = lines.flatMap(fmf);

        PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
            
            	String[] uber = s.split("::");
                return new Tuple2(uber[0], uber[1]);
            }
        };
        JavaPairRDD<String, String> ones = words.mapToPair(pf);

        Function2<String, String, String> f2 = new Function2<String, String, String>() {
            public String call(String x, String y) {
            	
            	String[] s1 = x.split(",");
            	String[] s2 = y.split(",");
            	
            	int[] sum = {0, 0};
            	
            	sum[0] = Integer.parseInt(s1[0]) + Integer.parseInt(s2[0]);
            	sum[1] = Integer.parseInt(s1[1]) + Integer.parseInt(s2[1]);
            	
               return Integer.toString(sum[0]) +","+ Integer.toString(sum[1]);
            }
        };
        JavaPairRDD<String, String> counts = ones.reduceByKey(f2);

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
