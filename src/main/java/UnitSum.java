import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.swing.plaf.multi.MultiInternalFrameUI;
import java.awt.geom.QuadCurve2D;
import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    /*
    MapReduce2-UnitSum:
    Mapper1: read output from MadReduce1
        'to_node' -> subPR
    Mapper2: read 'PR[i-1]'
        'from_node' -> pr[i-1]
    Reducer:
        'node' -> subPR + 'beta' * pr[i-1]
    */

    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: toPage \t unitMultiplication, "\t": tab
            //target: pass to reducer

            String[] toSubpr = value.toString().trim().split("\t");
            double subpr = Double.parseDouble(toSubpr[1]);
            context.write(new Text(toSubpr[0]), new DoubleWritable(subpr));
        }
    }

//    public static class BetaMapper extends  Mapper<Text, DoubleWritable, Text, DoubleWritable> {
//        float beta = 0;
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            beta = conf.getFloat("beta", 0.2f);
//        }
//
//        @Override
//        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
//            double betaPR = beta * value.get();
//            context.write(key, new DoubleWritable(betaPR));
//        }
//    }


    /* add 'BetaMapper' to read 'PageRank[i-1].txt'. Compute pr[i-1]*beta for Reducer */
    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float beta = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] prevPR = value.toString().trim().split("\t");
            double betaPR = beta * Double.parseDouble(prevPR[1]);

            context.write(new Text(prevPR[0]), new DoubleWritable(betaPR));
        }
    }


    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            //input key = toPage value = <unitMultiplication> + <BetaMapper>
            //target: sum!

            double sum = 0;
            for (DoubleWritable value: values) {
                sum += value.get();
            }

            /* format 'sum' to .4d */
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.parseDouble(df.format(sum));

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        /* chain two mapper together.
        The Mapper classes are invoked in a chained (or piped) fashion,
        the output of the first becomes the input of the second, and so on until the last Mapper,
        the output of the last Mapper will be written to the task's output.

        Hence, BetaMapper's inputKey & inputValue classes = PassMapper's outputKey & outputValue
        */
        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
