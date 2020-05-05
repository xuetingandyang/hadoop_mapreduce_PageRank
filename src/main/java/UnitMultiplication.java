import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {
    /*
    Implementation of PageRank with 'beta'.
    MP1-UnitMultiplication:
    Mapper1: 'from_node' -> 'to_node=probability'
    Mapper2: 'from_node' -> 'initial_pr'
    Reducer:
        input: 'from_node' -> 'to_node_1=prob_1', 'to_node_2=prob_2', 'to_node_3=prob_3', .., 'initial_pr'
        output: 'to_node'  -> (1-'beta') * 'initial_pr'
    */

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage \t toPage1,toPage2,toPage3 (delimiter: '\t')
            //target: build transition matrix unit -> fromPage \t toPage=probability
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");

            if (fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to: tos) {
                double prob = (double) 1 / tos.length;
                context.write(new Text(from), new Text(to + "=" + prob));
            }
        }
    }


    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page \t PageRank
            //target: write to reducer
            String line = value.toString().trim();
            String[] pagePR = line.split("\t");

            if (pagePR.length == 1) {
                return;
            }

            context.write(new Text(pagePR[0]), new Text(pagePR[1]));

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        float beta = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f); // if get null, default: beta=0.2
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication

            ArrayList<String> toProbs = new ArrayList<String>();
            double pr = 0;
            for (Text value: values) {
                if (value.toString().contains("=")) {
                    toProbs.add(value.toString().trim());
                }
                else {
                    pr = Double.parseDouble(value.toString().trim());
                }
            }

            for (String toProb: toProbs) {
                String to = toProb.split("=")[0];
                double prob = Double.parseDouble(toProb.split("=")[1]);

                String outputValue = String.valueOf((1 - beta) *prob * pr);

                context.write(new Text(to), new Text(outputValue));
            }


        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        /* chain two mapper together.
        The Mapper classes are invoked in a chained (or piped) fashion,
        the output of the first becomes the input of the second, and so on until the last Mapper,
        the output of the last Mapper will be written to the task's output.

        Hence, PRMapper's inputKey & inputValue classes = TransitionMapper's outputKey & outputValue
        Also, note that 'Text.class' is an extend of 'Object.class'
        */
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
