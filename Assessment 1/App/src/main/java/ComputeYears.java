import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ComputeYears {

    public static Schema getProjectionSchema() throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream is = fs.open(new Path("projection_schema.parquet"));
        byte[] Bytes = ByteStreams.toByteArray(is);
        String ps = new String(Bytes);
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static Schema getYearSchema() throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream is = fs.open(new Path("year_schema.parquet"));
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

    public static class MyReducer extends Reducer<Text, Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            schema = getYearSchema();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            GenericRecord record = new GenericData.Record(schema);

            int totalMovies = 0;                                     // total number of movies
            String mostVoted = "None";                               // tconst (id) of the most voted movie
            int mostVotedNum = -1;                                   // number of votes of the most voted movie
            List<Pair<String, Double>> topRated = new ArrayList<>(); // list with the top 10 best rated movies
            int min = 0;                                             // index of the movie with the lowest average rating in the list topRated
            List<String> topRatedIds = new ArrayList<>();            // list with the tconst (id) for each one of the top 10 best rated movies

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
                Collections.sort(topRated, new FromParquet.MyComparator());

                // update the total number of movies
                totalMovies += 1;
            }

            // get the tconst (id) for each one of the top 10 best rated movies
            for (int i = 0; i < 10; i++) {
                if (topRated.get(i).getValue() > 0)
                    topRatedIds.add(topRated.get(i).getKey());
            }

            record.put("year", Integer.parseInt(key.toString()));
            record.put("totalMovies", totalMovies);
            record.put("mostVoted", mostVoted);
            record.put("topRated", topRatedIds);

            context.write(null, record);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "ComputeYears");

        // Set the jar where this classes are
        job.setJarByClass(ComputeYears.class);

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
        AvroParquetInputFormat.setRequestedProjection(job, getProjectionSchema());

        // Output configuration
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getYearSchema());
        FileOutputFormat.setOutputPath(job, new Path("compute_years_output"));

        // Execute job
        job.waitForCompletion(true);
    }
}
