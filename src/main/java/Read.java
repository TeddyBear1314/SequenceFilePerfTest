import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import sun.management.FileSystem;

import java.io.IOException;

/**
 * Created by dell on 2016/12/29.
 */
public class Read {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();

        SequenceFile.Reader.Option filePath = SequenceFile.Reader.file(new Path(uri));
        SequenceFile.Reader reader = null;
        long beginTime = System.currentTimeMillis();
        try {
            reader = new SequenceFile.Reader(conf,filePath);
            Writable key = (Writable) ReflectionUtils.newInstance(
                reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - beginTime);

    }
}
