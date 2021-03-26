import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.util.List;

public class FromParquet {

    public static class MyMapper extends Mapper<Void, GenericRecord, Text, LongWritable> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            List<String> genres = (List<String>)value.get("genres");
            for (String genre : genres) {
                context.write(new Text(genre), new LongWritable(1));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable value : values) {
                total += value.get();
            }
            context.write(key, new Text(Long.toString(total)));
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
        job.setMapOutputValueClass(LongWritable.class);

        // Input configuration
        job.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("to_parquet_output"));

        // Output configuration
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("from_parquet_output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute job
        job.waitForCompletion(true);
    }
}
