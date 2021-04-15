import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.util.List;

public class ValidateYears {

    public static class MyMapper extends Mapper<Void, GenericRecord, Text, Text> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            int year = (Integer)value.get("year");
            int totalMovies = (Integer)value.get("totalMovies");
            String mostVoted = (String)value.get("mostVoted");
            List<String> topRated = (List<String>)value.get("topRated");

            StringBuilder sb = new StringBuilder();

            sb.append("\n\nNumber of movies: " + totalMovies + "\n\n");

            sb.append("Movie with the most votes: " + mostVoted + "\n");

            sb.append("\nTop 10 movies by average rating:\n");
            for (int i = 0; i < topRated.size(); i++) {
                sb.append("\n  > " + topRated.get(i) + "\n");
            }

            sb.append("\n----------\n");

            context.write(new Text(year + ""), new Text(sb.toString()));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(key, new Text(value));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "ValidateYears");

        // Set the jar where this classes are
        job.setJarByClass(ValidateYears.class);

        // Mapper configuration
        job.setMapperClass(MyMapper.class);

        // Reducer configuration
        job.setReducerClass(MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input configuration
        job.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("compute_years_output"));

        // Output configuration
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("validate_years_output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute job
        job.waitForCompletion(true);
    }
}
