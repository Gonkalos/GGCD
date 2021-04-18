import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RatingsSorted {

    public static class MyComparator implements Comparator<Tuple2<String, Tuple2<String, Double>>> {
        @Override
        public int compare(Tuple2<String,Tuple2<String,Double>> value1, Tuple2<String,Tuple2<String,Double>> value2) {
            if (value1._2._2 > value2._2._2) return -1;
            else return 1;
        }
    }

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
                // filter ratings >= 9.0
                .filter(l -> Double.parseDouble(l[1]) >= 9.0)
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // merge outputs
        List<Tuple2<String, Tuple2<String, Double>>> values = basics.join(ratings).collect();

        // sort output
        List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<>(values);
        Collections.sort(list, new MyComparator());

        for (Tuple2<String, Tuple2<String, Double>> value : list) {
            System.out.println(value._2._1 + ": average rating = " + value._2._2);
        }
    }
}
