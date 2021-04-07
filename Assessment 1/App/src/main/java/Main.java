import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /*
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - Tarefa 1 - Carregamento dos Dados para AvroParquet  - - - - - - - - - - - - - - - - -
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        */

        // Create new job
        Job job1 = Job.getInstance(new Configuration(), "ToParquet");

        // Set the jar where this classes are
        job1.setJarByClass(ToParquet.class);

        // Mapper configuration
        job1.setMapperClass(ToParquet.BasicsMapper.class);
        job1.setMapperClass(ToParquet.RatingsMapper.class);

        // Reducer configuration
        job1.setReducerClass(ToParquet.MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // Input configuration
        MultipleInputs.addInputPath(job1, new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.basics.tsv.bz2"), TextInputFormat.class, ToParquet.BasicsMapper.class);
        MultipleInputs.addInputPath(job1, new Path("/Users/goncalo/Documents/University/GGCD/Classes/IMDb Datasets/Mini/title.ratings.tsv.bz2"), TextInputFormat.class, ToParquet.RatingsMapper.class);

        // Output configuration
        job1.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job1, ToParquet.getSchema());
        FileOutputFormat.setOutputPath(job1, new Path("to_parquet_output"));

        // Execute job
        job1.waitForCompletion(true);

        /*
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - Validação da Tarefa 1 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        */

        // Create new job
        Job job2 = Job.getInstance(new Configuration(), "FromParquet");

        // Set the jar where this classes are
        job2.setJarByClass(FromParquet.class);

        // Mapper configuration
        job2.setMapperClass(FromParquet.MyMapper.class);

        // Reducer configuration
        job2.setReducerClass(FromParquet.MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // Input configuration
        job2.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path("to_parquet_output"));
        AvroParquetInputFormat.setRequestedProjection(job2, FromParquet.getSchema());

        // Output configuration
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("from_parquet_output"));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Execute job
        job2.waitForCompletion(true);

        /*
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - Tarefa 2 - Computação dos Dados por Ano - - - - - - - - - - - - - - - - - - - - - - -
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        */

        // Create new job
        Job job3 = Job.getInstance(new Configuration(), "ComputeYears");

        // Set the jar where this classes are
        job3.setJarByClass(ComputeYears.class);

        // Mapper configuration
        job3.setMapperClass(ComputeYears.MyMapper.class);

        // Reducer configuration
        job3.setReducerClass(ComputeYears.MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        // Input configuration
        job3.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job3, new Path("to_parquet_output"));
        AvroParquetInputFormat.setRequestedProjection(job3, ComputeYears.getProjectionSchema());

        // Output configuration
        job3.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job3, ComputeYears.getYearSchema());
        FileOutputFormat.setOutputPath(job3, new Path("compute_years_output"));

        // Execute job
        job3.waitForCompletion(true);

        /*
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - Validação da Tarefa 2 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        */

        // Create new job
        Job job4 = Job.getInstance(new Configuration(), "ValidateYears");

        // Set the jar where this classes are
        job4.setJarByClass(ValidateYears.class);

        // Mapper configuration
        job4.setMapperClass(ValidateYears.MyMapper.class);

        // Reducer configuration
        job4.setReducerClass(ValidateYears.MyReducer.class);

        // Data communicated between Mapper and Reducer configuration
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        // Input configuration
        job4.setInputFormatClass(AvroParquetInputFormat.class);
        TextInputFormat.setInputPaths(job4, new Path("compute_years_output"));

        // Output configuration
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path("validate_years_output"));
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        // Execute job
        job4.waitForCompletion(true);
    }
}
