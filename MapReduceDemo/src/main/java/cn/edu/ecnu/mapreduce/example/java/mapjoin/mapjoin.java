package cn.edu.ecnu.mapreduce.example.java.mapjoin;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class mapjoin extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception  {
        Job job =Job.getInstance(getConf(),getClass().getSimpleName());
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(mapjoinmapper.class);
        job.setReducerClass(mapjoinreducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0:1;
    }
    public  static void main(String[] args) throws Exception{
        long begin=System.currentTimeMillis();
        int exitCode =ToolRunner.run(new mapjoin(),args);
        long end=System.currentTimeMillis();
        System.out.println("程序运行的时间为："+(end-begin)+"ms");
        System.exit(exitCode);

    }
}
