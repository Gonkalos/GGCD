import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopActors {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopActorsByMovieCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse name.basics file to create pairs (tconst, nconst) only for actors
        JavaPairRDD<String, String> name_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/name.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // filter actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // create list of pairs (nconst, [tconst, ...])
                .mapToPair(l -> new Tuple2<>(l[0], Arrays.asList(l[5].split(",")).iterator()))
                // flat pairs to [(nconst, tconst), ...]
                .flatMapValues(l -> l)
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l._2, l._1))
                // caching
                .cache();

        // parse title.basics file to create pairs (tconst, "") only for movies
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

        // join RDDs name_basics and title_basics
        List<Tuple2<String, Integer>> values = name_basics.join(title_basics) // (tconst, (nconst, ""))
                .map(l -> new Tuple2<>(l._2._1, l._1))                        // (nconst, tconst)
                .mapToPair(l -> new Tuple2<>(l._1, 1))                        // (nconst, 1)
                .reduceByKey((a, b) -> a + b)                                 // (nconst, number_of_movies)
                .mapToPair(p -> p.swap())                                     // (number_of_movies, nconst)
                .sortByKey(false)                                             // sort by number of movies
                .mapToPair(p -> p.swap())                                     // (nconst, number_of_movies)
                .collect();

        // show results
        System.out.println("Top 10 actors by number of movies:\n");
        for (int i = 0; i < 10; i++) {
            Tuple2<String, Integer> value = values.get(i);
            System.out.println("#" + (i + 1) + ": " + value._1 + "(" + value._2 + " movies)");
        }
    }
}
