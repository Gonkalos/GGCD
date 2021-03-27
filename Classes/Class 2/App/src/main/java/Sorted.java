import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sorted {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().equals("tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres")) {
                String[] data = value.toString().split("\t");
                context.write(new Text(data[0]), new Text("title:" + data[2]));
            }
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().equals("tconst\taverageRating\tnumVotes")) {
                String[] data = value.toString().split("\t");
                float rating = -1;
                try {
                    rating = Float.parseFloat(data[1]);
                    if (rating >= 9) {
                        context.write(new Text(data[0]), new Text("rating:" + data[1]));
                    }
                }
                catch (NumberFormatException e) {
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = "";
            String rating = "";
            for (Text value : values) {
                if (value.toString().contains(":")) {
                    if (value.toString().startsWith("title")) title = value.toString().split(":")[1];
                    else rating = value.toString().split(":")[1];
                    if (!rating.equals("")) context.write(key, new Text(title + ", " + rating));
                }
            }
        }
    }

    public static class MyMapper3 extends Mapper<LongWritable, Text, FloatWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t")[1].split(", ");
            float rating = -1;
            try {
                rating = Float.parseFloat(data[1]);
                context.write(new FloatWritable(rating), new Text(data[0]));
            }
            catch (NumberFormatException e) {
            }
        }
    }

    public static class MyReducer2 extends Reducer<FloatWritable, Text, FloatWritable, Text> {

        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, new Text(value));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // ------------------------------ Job 1 -------------------------------------

        // Create new job
        Job job = Job.getInstance(new Configuration(), "filter");

        // Set the jar where this classes are
        job.setJarByClass(Sorted.class);

        // Mapper configuration
        job.setMapperClass(MyMapper.class);
        job.setMapperClass(MyMapper2.class);

        // Reducer configuration
        job.setReducerClass(MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input configuration
        MultipleInputs.addInputPath(job,new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.basics.tsv.bz2"), TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job,new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.ratings.tsv.bz2"), TextInputFormat.class, MyMapper2.class);

        // Output configuration
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute job
        job.waitForCompletion(true);

        // ------------------------------ Job 2 -------------------------------------

        // Create new job
        Job job2 = Job.getInstance(new Configuration(), "sort");

        // Set the jar where this classes are
        job2.setJarByClass(Sorted.class);

        // Mapper configuration
        job2.setMapperClass(MyMapper3.class);

        // Reducer configuration
        job2.setReducerClass(MyReducer2.class);

        // Data communicated between Mapper and Reducer configuration
        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // Input configuration
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path("./output/part-r-00000"));

        // Output configuration
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("sorted"));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Execute job
        job2.waitForCompletion(true);
    }
}
