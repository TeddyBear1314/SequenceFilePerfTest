import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

/**
 * Created by dell on 2016/12/28.
 */
public class  Write {
    private static Random rand =  new Random();
    private static final int maxValueLength = 5000;
    private static final String DATA = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890";

    private static String genRandomString() {
        int num = rand.nextInt(maxValueLength);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < num; i++) {
            sb.append(DATA.charAt(rand.nextInt(DATA.length())));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        int num = Integer.parseInt(args[1]);
        String codec = args[2];
        Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        CompressionCodec co = null;


        if (codec.equals("gzip")) {
           co= new GzipCodec();
        } else if (codec.equals("snappy")) {
            co = new SnappyCodec();
        } else if (codec.equals("lz4")) {
            co =new Lz4Codec();
        } else if (codec.equals("bzip2")) {
            co =new BZip2Codec();
        } else {
            co = new DefaultCodec();
        }
        try {
            SequenceFile.Writer.Option[] options = new SequenceFile.Writer.Option[]{SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, co), SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass())};
            writer =SequenceFile.createWriter(conf, options);//为什么不能设置fs
            for (int i = 0; i < num; i ++) {
                key.set(i);
                value.set(genRandomString());
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
