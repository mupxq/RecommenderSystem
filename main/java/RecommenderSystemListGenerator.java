/**
 * Created by mupxq on 10/13/16.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecommenderSystemListGenerator {

        public class RecommenderSystemListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

            Map<Integer,List<Integer>> watchHistory = new HashMap<Integer, List<Integer>>();

            @Override
            public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
                String filePath = conf.get("WatchHistory");

                Path pt =new Path(filePath);

                FileSystem fs = FileSystem.get(conf);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line = br.readLine();

                while (line != null){
                    int user = Integer.parseInt(line.split(",")[0]);
                    int movie = Integer.parseInt(line.split(",")[0]);
                    if (watchHistory.containsKey(user)){
                        watchHistory.get(user).add(movie);

                    }
                    else {
                        List<Integer> list = new ArrayList<Integer>();
                        list.add(movie);
                        watchHistory.put(user, list);
                    }
                    line = br.readLine();
                }
                br.close();;

            }

            @Override
            public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
                String[] tokens = value.toString().split("\t");
                int user = Integer.parseInt(tokens[0]);
                int movie = Integer.parseInt(tokens[1]);
                if (!watchHistory.containsKey(movie)){
                    context.write(new IntWritable(user), new Text(movie + ":" + tokens[2]));
                }


            }
        }


        public class RecommenderSystemListGeneratorReducer extends Reducer<IntWritable, Text,  IntWritable,Text> {

            Map<Integer, String> movieTitle = new HashMap<Integer, String>();


            @Override
            public void setup(Context context) throws IOException {
                Configuration conf = context.getConfiguration();
                String filePath = conf.get("movieTitles");

                Path pt =new Path(filePath);

                FileSystem fs = FileSystem.get(conf);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line = br.readLine();

                while (line != null){
                    int movie_id = Integer.parseInt(line.trim().split(",")[0]);
                    movieTitle.put(movie_id, line.trim().split(",")[1]);
                    line = br.readLine();
                }
                br.close();
            }

            @Override
            public void reduce(IntWritable key,Iterable<Text> values, Context context) throws IOException, InterruptedException {

                while (values.iterator().hasNext()){
                    String cur = values.iterator().next().toString();

                    int movie_id = Integer.parseInt(cur.split(":")[0]);
                    String rating = cur.split(":")[1];

                    context.write(key,new Text(movieTitle.get(movie_id) + ";" +rating));
                }


            }
        }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("coOccurrencePath",args[0]);

        Job job = Job.getInstance();
        job.setMapperClass(RecommenderSystemListGeneratorMapper.class);
        job.setReducerClass(RecommenderSystemListGeneratorReducer.class);

        job.setJarByClass(RecommenderSystemListGenerator.class);
        job.setNumReduceTasks(3);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);



        TextInputFormat.setInputPaths(job, new Path(args[2]));
        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
    }
}
