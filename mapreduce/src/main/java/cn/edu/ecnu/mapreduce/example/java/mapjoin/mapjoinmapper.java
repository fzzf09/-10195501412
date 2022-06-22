package cn.edu.ecnu.mapreduce.example.java.mapjoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class mapjoinmapper extends Mapper<LongWritable,Text,Text, NullWritable> {
    private Text val = new Text();
    private Map<String,String> departmenttable=new HashMap<>();

    protected void map(LongWritable key, Text value,Context context)
        throws IOException,InterruptedException{
        if(departmenttable.isEmpty()){
            URI uri=context.getCacheFiles()[0];
            FileSystem fs=FileSystem.get(uri,new Configuration());
            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            String content;
            while ((content=reader.readLine())!= null){
                String[] datas=content.split(" ");
                departmenttable.put(datas[0],datas[1]);
            }

        }
        String[] data1=value.toString().split(" ");
        String name=data1[1];
        if(departmenttable.containsKey(name)){
            val.set(value.toString()+" "+departmenttable.get(name));
            context.write(val,  NullWritable.get());

        }
    }

}
