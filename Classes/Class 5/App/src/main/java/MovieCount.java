import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MovieCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("MovieCountByGenres");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse title.basics file
        JavaPairRDD<String, Integer> basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // filter movies
                .filter(l -> l[1].equals("movie"))
                // choose atribute genres
                .map(l -> l[8])
                // ignore null values
                .filter(l -> !l.equals("\\N"))
                // split genres
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                // create pairs (genre, 1)
                .mapToPair(l -> new Tuple2<>(l, 1))
                // reduce operation
                .reduceByKey((v1, v2) -> v1 + v2);

        // run the job
        List<Tuple2<String, Integer>> genres = basics.collect();

        for (Tuple2<String, Integer> genre : genres) {
            System.out.println(genre._1 + ": " + genre._2 + " movies");
        }
    }
}
