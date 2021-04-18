import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ActorMovieCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ActorMovieCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse name.basics file
        JavaPairRDD<String, String> name_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/name.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // filter actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // create list of pairs (tconst, primaryName)
                .map(l -> {
                    String[] titles = l[5].split(",");
                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    for (String title : titles) {
                        pairs.add(new Tuple2<>(title, l[1]));
                    }
                    return pairs;
                })
                // iterate over list of pairs (tconst, primaryName)
                .flatMap(l -> l.iterator())
                // create pairs (tconst, primaryName)
                .mapToPair(l -> new Tuple2<>(l._1, l._2))
                // caching
                .cache();

        // parse title.basics file
        JavaPairRDD<String, String> title_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // filter movies
                .filter(l -> l[1].equals("movie"))
                // create pairs (tconst, "")
                .mapToPair(l -> new Tuple2<>(l[0], ""))
                // caching
                .cache();

        // merge the outputs
        List<Tuple2<String, Integer>> values = name_basics.join(title_basics) // (tconst, (actor_name, ""))
                .map(l -> new Tuple2<>(l._2._1, l._1))                        // (actor_name, tconst)
                .mapToPair(l -> new Tuple2<>(l._1, 1))                        // (actor_name, 1)
                .reduceByKey((a, b) -> a + b)                                 // (actor_name, number_of_movies)
                .collect();

        for (Tuple2<String, Integer> value : values) {
            System.out.println(value._1 + ": " + value._2 + " movies");
        }
    }
}
