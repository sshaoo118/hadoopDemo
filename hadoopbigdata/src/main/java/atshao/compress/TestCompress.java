package atshao.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        compress("e:/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
    }

    private static void compress(String filename,String method) throws IOException, ClassNotFoundException {

        FileInputStream fis = new FileInputStream(new File(filename));
        Class codecClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis,cos,1024*1024,false);

        cos.close();
        fos.close();
        fis.close();
    }

    private static void decompress(String filename){
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null){
            System.out.println("cannot find codec for file" + filename);
            return;
        }

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));
        new FileOutputStream()

    }
}
