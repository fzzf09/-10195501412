package cn.edu.ecnu.mapreduce.example.java.mapjoin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class mapjoinreducer extends Reducer<IntWritable,Text,Text, NullWritable> {
    private Text manager = new Text();
    private Text value = new Text();
    protected void reduce(IntWritable key, Iterable<Text> values,Context context)
        throws  IOException,InterruptedException{
        List<String> grades = new ArrayList<String>();
        for (Text val : values) {
            String[] datas=val.toString().split(" ");
            if (val.toString().contains("input2.txt")) {
                manager.set(datas[1]);
            } else {
                grades.add(datas[1]+" "+datas[2]);
            }
        }
        for (String grade : grades) {
            value.set(grade+" "+manager.toString());
            context.write(value, NullWritable.get());
        }
    }


}
