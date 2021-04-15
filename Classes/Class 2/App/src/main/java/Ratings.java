import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Ratings {

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
                context.write(new Text(data[0]), new Text("rating:" + data[1]));
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
                    if (rating.equals("")) rating = "N/A";
                    context.write(key, new Text(title + ", " + rating));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "ratings");

        // Set the jar where this classes are
        job.setJarByClass(Ratings.class);

        // Mapper configuration
        job.setMapperClass(MyMapper.class);
        job.setMapperClass(MyMapper2.class);

        // Reducer configuration
        job.setReducerClass(MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Input configuration
        MultipleInputs.addInputPath(job,new Path("/Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2"), TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job,new Path("/Users/goncalo/Documents/University/GGCD/Classes/Data/title.ratings.tsv.bz2"), TextInputFormat.class, MyMapper2.class);

        // Output configuration
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute job
        job.waitForCompletion(true);
    }
}
