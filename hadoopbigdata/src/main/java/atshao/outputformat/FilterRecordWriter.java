package atshao.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text,NullWritable> {

    FSDataOutputStream atguiguOut;
    FSDataOutputStream otherOut;

    public FilterRecordWriter (TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());

        atguiguOut = fs.create(new Path("e:/atguigu.log"));

        otherOut = fs.create(new Path("e:/other.log"));
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {

        if (key.toString().contains("atguigu")){
            atguiguOut.write(key.toString().getBytes());
        }else {
            otherOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
