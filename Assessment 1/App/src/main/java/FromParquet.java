import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.*;

public class FromParquet {

    public static Schema getSchema() throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream is = fs.open(new Path("projection_schema.parquet"));
        byte[] Bytes = ByteStreams.toByteArray(is);
        String ps = new String(Bytes);
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class MyMapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            String titleType = (String)value.get("titleType");
            String startYear = (String)value.get("startYear");

            if (titleType.equals("movie") && !startYear.equals("\\N")) {

                String tconst = (String)value.get("tconst");
                Double averageRating = (Double)value.get("averageRating");
                int numVotes = (Integer)value.get("numVotes");
                context.write(new Text(startYear), new Text(tconst + "\t" + averageRating + "\t" + numVotes));
            }
        }
    }

    public static class MyComparator implements Comparator<Pair<String, Double>> {
        @Override
        public int compare(Pair<String, Double> pair1, Pair<String, Double> pair2) {
            if (pair1.getValue() > pair2.getValue()) return -1;
            else return 1;
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int totalMovies = 0;                                     // total number of movies
            String mostVoted = "None";                               // tconst (id) of the most voted movie
            int mostVotedNum = -1;                                   // number of votes of the most voted movie
            List<Pair<String, Double>> topRated = new ArrayList<>(); // list with the top 10 best rated movies
            int min = 0;                                             // index of the movie with the lowest average rating in the list topRated

            // fill the list topRated
            for (int i = 0; i < 10; i++) {
                topRated.add(i, new Pair<>("", -1.0));
            }

            for (Text value : values) {

                // get the info from each movie
                String[] data = value.toString().split("\t");
                String tconst = data[0];
                double averageRating = Double.parseDouble(data[1]);
                int numVotes = Integer.parseInt(data[2]);

                // update the most voted movie
                if (numVotes > mostVotedNum) {
                    mostVoted = tconst;
                    mostVotedNum = numVotes;
                }

                // update the index of the movie with the lowest average rating
                for (int i = 0; i < 10; i++) {
                    if (topRated.get(i).getValue() < topRated.get(min).getValue())
                        min = i;
                }

                // update the list topRated
                if (averageRating > topRated.get(min).getValue()) topRated.set(min, new Pair<>(tconst, averageRating));

                // sort the list topRated
                Collections.sort(topRated, new MyComparator());

                // update the total number of movies
                totalMovies += 1;
            }

            StringBuilder sb = new StringBuilder();

            sb.append("\n\nNumber of movies: " + totalMovies + "\n");

            if (mostVotedNum > 0) {
                sb.append("\nMovie with the most votes: " + mostVoted + " (" + mostVotedNum + " votes)\n");
            }
            else {
                sb.append("\nMovie with the most votes: " + mostVoted + "\n");
            }

            sb.append("\nTop 10 movies by average rating:\n");
            for (int i = 0; i < 10; i++) {
                if (topRated.get(i).getValue() >= 0) {
                    sb.append("\n  > " + topRated.get(i).getKey() + " (average rating = " + topRated.get(i).getValue() + ")\n");
                }
            }
            sb.append("\n----------\n");

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "FromParquet");

        // Set the jar where this classes are
        job.setJarByClass(FromParquet.class);

        // Mapper configuration
        job.setMapperClass(MyMapper.class);

        // Reducer configuration
        job.setReducerClass(MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input configuration
        job.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("to_parquet_output"));
        AvroParquetInputFormat.setRequestedProjection(job, getSchema());

        // Output configuration
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("from_parquet_output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute job
        job.waitForCompletion(true);
    }
}
