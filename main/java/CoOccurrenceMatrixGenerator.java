/**
 * Created by mupxq on 10/12/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CoOccurrenceMatrixGenerator {
    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] user_movieRating = line.split("\t");
            String user = user_movieRating[0];
            String[] movie_rating = user_movieRating[1].split(",");

            for (int i =0; i < movie_rating.length; i++){
                String movie1 = movie_rating[i].trim().split(":")[0];
                for (int j = 0; j < movie_rating.length; j++){
                    String movie2 = movie_rating[j].trim().split(":")[0];
                    context.write(new Text(movie1 + ":" +movie2), new IntWritable(1));
                }
            }

        }
    }


    public static class  MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
        while (values.iterator().hasNext()){
            sum += values.iterator().next().get();
        }

        context.write(key, new IntWritable(sum));

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance();
        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);
        job.setNumReduceTasks(3);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
