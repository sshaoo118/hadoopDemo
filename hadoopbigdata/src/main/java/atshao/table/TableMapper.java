package atshao.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    String name;
    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (name.startsWith("order")){

            String[] fields = line.split("\t");

            v.setOrder_id(fields[0]);
            v.setP_id(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("order");

            k.set(fields[1]);
        }else {

            String[] fields = line.split("\t");
            v.setOrder_id("");
            v.setP_id(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("pd");

            k.set(fields[0]);
        }

        context.write(k,v);
    }
}
