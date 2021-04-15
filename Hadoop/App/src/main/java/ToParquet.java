import com.google.common.io.ByteStreams;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ToParquet {

    public static Schema getSchema() throws IOException, OutOfMemoryError {

        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream is = fs.open(new Path("movie_schema.parquet"));
        byte[] Bytes = ByteStreams.toByteArray(is);
        String ps = new String(Bytes);
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class BasicsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (!value.toString().equals("tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres")) {

                String tconst = value.toString().split("\t")[0];
                String content = value.toString().split(tconst + "\t")[1];

                context.write(new Text(tconst), new Text("basics%&" + content));
            }
        }
    }

    public static class RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (!value.toString().equals("tconst\taverageRating\tnumVotes")) {

                String tconst = value.toString().split("\t")[0];
                String content = value.toString().split(tconst + "\t")[1];

                context.write(new Text(tconst), new Text("ratings%&" + content));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            schema = getSchema();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            GenericRecord record = new GenericData.Record(schema);

            String basics = "";
            String ratings = "";

            boolean ready = false;        // flag to check if the record is valid
            boolean ratingExists = false; // flag to check if the movie has rating

            for (Text value : values) {

                if (value.toString().contains("%&")) {
                    if (value.toString().startsWith("basics")) basics = value.toString().split("%&")[1];
                    else ratings = value.toString().split("%&")[1];
                }

                String[] basics_data = basics.split("\t");

                if (basics_data.length >= 8) {

                    record.put("tconst", key.toString());
                    record.put("titleType", basics_data[0]);
                    record.put("primaryTitle", basics_data[1]);
                    record.put("originalTitle", basics_data[2]);
                    record.put("isAdult", basics_data[3]);
                    record.put("startYear", basics_data[4]);
                    record.put("endYear", basics_data[5]);
                    record.put("runtimeMinutes", basics_data[6]);
                    List<String> genres = new ArrayList<>();
                    for (String genre : basics_data[7].split(",")) {
                        genres.add(genre);
                    }
                    record.put("genres", genres);

                    ready = true;
                }

                String[] ratings_data = ratings.split("\t");

                if (ratings_data.length >= 2) {

                    record.put("averageRating", Double.parseDouble(ratings_data[0]));
                    record.put("numVotes", Integer.parseInt(ratings_data[1]));

                    ratingExists = true;
                }
            }

            if (!ratingExists) {
                record.put("averageRating", -1.0);
                record.put("numVotes", -1);
            }

            if (ready) context.write(null, record);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "ToParquet");

        // Set the jar where this classes are
        job.setJarByClass(ToParquet.class);

        // Mapper configuration
        job.setMapperClass(BasicsMapper.class);
        job.setMapperClass(RatingsMapper.class);

        // Reducer configuration
        job.setReducerClass(MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input configuration
        MultipleInputs.addInputPath(job, new Path("/Users/goncalo/Documents/University/GGCD/Hadoop/Data/title.basics.tsv.gz"), TextInputFormat.class, BasicsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/goncalo/Documents/University/GGCD/Hadoop/Data/title.ratings.tsv.gz"), TextInputFormat.class, RatingsMapper.class);

        // Output configuration
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getSchema());
        FileOutputFormat.setOutputPath(job, new Path("to_parquet_output"));

        // Execute job
        job.waitForCompletion(true);
    }
}
