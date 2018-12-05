package atshao.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholeRecordReader extends RecordReader<Text,BytesWritable> {

    private Configuration conf;
    private FileSplit split;

    private boolean isProgress = true;
    private BytesWritable value = new BytesWritable();
    private Text k = new Text();
    private FSDataInputStream fis;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext conext)  {
        this.split = (FileSplit) split;
        conf = conext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue()  {
        if (isProgress){
            byte[] contents = new byte[(int) split.getLength()];
            FileSystem fs = null;
            try {
                Path path = split.getPath();
                fs = path.getFileSystem(conf);
                fis = fs.open(path);
                IOUtils.readFully(fis,contents,0,contents.length);
                value.set(contents,0,contents.length);
                String name = split.getPath().toString();
                k.set(name);
            }catch (Exception e){

            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey()  {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress()  {
        return 0;
    }

    @Override
    public void close() {

    }
}
