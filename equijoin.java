
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;


/**
 * Created by yutang on 11/12/17.
 */

public class equijoin {
    static Text tableName1 = new Text();
    static Text tableName2 = new Text();
    /**
     * Inside main function, define the Job for hadoop. Each hadoop job has at least one mapper
     * and one reducer, however, you might have more mappers and reducers in different scenarios.
     * For the job, need to configure the inputFormat and outputFormat,
     * as well as for mappers and reducers, define the input/output key/value format
     * @param argus input and output file path
     *              argus[0] inputPath
     *              argus[1] outputPath
     */
    public static void main(String[] argus) throws IOException, ClassNotFoundException, InterruptedException {
        //create an instance of Configuration for job
        Configuration configuration = new Configuration();
        //create job with configuration and name
        Job job = new Job(configuration);
        //set input and output file path, path must be an instance of org.apache.hadoop.fs.Path{}
        FileInputFormat.addInputPath(job, new Path(argus[0]));
        FileOutputFormat.setOutputPath(job, new Path(argus[1]));
        /**
         * set input and output format class, specifically for mapper, reducer will obtain data from mapper
         * see difference between TextInputFormat and KeyValueTextInputFormat on Hadoop API
         */
        job.setInputFormatClass(TextInputFormat.class);//corresponding with Mapper's first two parameter, keyIn and ValueIn
        job.setOutputFormatClass(TextOutputFormat.class);//Corresponding with Reducer
        // set mapper and reducer class
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        /**
         * set outputKey outputValue class, if mapper's output is different from reducer's,
         * need to explicitly call setMapOutputKeyClass() and setMapOutputValueClass()
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    /**
     * The Mapper class, where parameters are KeyIn, ValueIn, KeyOut, ValueOut
     * Here since in main(), set the InputFormatClass(TextInputFormat.class), need IntWritable as KeyIn here.
     */
    static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         *
         * @param key the offset of each line
         * @param value all information in value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text outputKey = new Text();
            Text outputValue = new Text();
            String[] values = value.toString().split(",");
            if (tableName1.toString().isEmpty()) {
                tableName1.set(values[0]);
            } else if (tableName2.toString().isEmpty() && !values[0].equals(tableName1.toString())) {
                tableName2.set(values[0]);
            }
            outputKey.set(values[1].trim());
            outputValue.set(value.toString());
            context.write(outputKey, outputValue);
        }
    }


    static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text outputValue = new Text();
            //use a list to store splited strings from values to determine R and S
            ArrayList<String> table1 = new ArrayList<>();
            ArrayList<String> table2 = new ArrayList<>();
            for (Text value : values) {
                if (value.toString().split(",")[0].trim().equals(tableName1.toString())) {
                    table1.add(value.toString());
                } else {
                    table2.add(value.toString());
                }
            }
            for (int i = 0; i < table1.size(); i++) {
                for (int j = 0; j < table2.size(); j++) {
                    outputValue.set(table1.get(i) + "," + table2.get(j));
                    context.write(null, outputValue);
                }
            }
        }
    }


}
