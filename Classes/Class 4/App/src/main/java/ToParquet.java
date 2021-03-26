import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

    public static class MyMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            schema = getSchema();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            GenericRecord record = new GenericData.Record(schema);

            if (!value.toString().equals("tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres")) {

                String[] data = value.toString().split("\t");
                record.put("tconst", data[0]);
                record.put("titleType", data[1]);
                record.put("primaryTitle", data[2]);
                record.put("originalTitle", data[3]);
                record.put("isAdult", data[4]);
                record.put("startYear", data[5]);
                record.put("endYear", data[6]);
                record.put("runtimeMinutes", data[7]);
                List<String> genres = new ArrayList<>();
                for (String genre : data[8].split(",")) {
                    genres.add(genre);
                }
                record.put("genres", genres);

                context.write(null, record);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Create new job
        Job job = Job.getInstance(new Configuration(), "ToParquet");

        // Set the jar where this classes are
        job.setJarByClass(ToParquet.class);

        // Mapper configuration
        job.setMapperClass(MyMapper.class);

        // Reducer configuration
        job.setNumReduceTasks(0);

        // Mapper output configuration
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        // Input configuration
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.basics.tsv.bz2"));

        // Output configuration
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getSchema());
        FileOutputFormat.setOutputPath(job, new Path("to_parquet_output"));

        // Execute job
        job.waitForCompletion(true);
    }
}