package cn.edu.ecnu.mapreduce.example.java.mapjoin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
public class mapjoinmapper extends Mapper<LongWritable,Text,IntWritable,Text> {
    private IntWritable stuKey = new IntWritable();
    private Text val = new Text();

    protected void map(LongWritable key, Text value,Context context)
        throws IOException,InterruptedException{
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();
        String[] datas=value.toString().split(" ");
        if(name.equals("input2.txt")){
            stuKey.set(Integer.parseInt(datas[0]));
            val.set(name+" "+datas[1]);
        }
        if(name.equals("input1.txt")){
            stuKey.set(Integer.parseInt(datas[1]));
            val.set(name+" "+value.toString());
        }
        context.write(stuKey, val);
    }

}
