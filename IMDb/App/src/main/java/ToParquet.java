import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ToParquet {

    public static Schema getSchema() throws IOException {

        InputStream is = new FileInputStream("schema.parquet");
        String ps = new String(is.readAllBytes());
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

            for (Text value : values) {

                if (value.toString().contains("%&")) {
                    if (value.toString().startsWith("basics")) basics = value.toString().split("%&")[1];
                    else ratings = value.toString().split("%&")[1];
                }

                if (!basics.equals("") && !ratings.equals("")) {

                    String[] basics_data = basics.split("\t");
                    String[] ratings_data = ratings.split("\t");

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
                    record.put("averageRating", Float.parseFloat(ratings_data[0]));
                    record.put("numVotes", Integer.parseInt(ratings_data[1]));

                    context.write(null, record);
                }
            }
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
        MultipleInputs.addInputPath(job, new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.basics.tsv.bz2"), TextInputFormat.class, BasicsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.ratings.tsv.bz2"), TextInputFormat.class, RatingsMapper.class);

        // Output configuration
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getSchema());
        FileOutputFormat.setOutputPath(job, new Path("to_parquet_output"));

        // Execute job
        job.waitForCompletion(true);
    }
}
