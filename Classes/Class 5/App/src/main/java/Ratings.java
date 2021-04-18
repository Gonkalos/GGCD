import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Ratings {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("MovieRatings");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse title.basics file
        JavaPairRDD<String, String> basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // filter movies
                .filter(l -> l[1].equals("movie"))
                // create pairs (tconst, primaryTitle)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // parse title.ratings file
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.ratings.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // merge outputs
        List<Tuple2<String, Tuple2<String, Double>>> values = basics.join(ratings).collect();

        for (Tuple2<String, Tuple2<String, Double>> value : values) {
            System.out.println(value._2._1 + ": average rating = " + value._2._2);
        }
    }
}
