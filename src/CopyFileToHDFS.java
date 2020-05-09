import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class CopyFileToHDFS {
  public static void main(String[] args) throws IOException, URISyntaxException 
   {
      //1. Get the instance of COnfiguration
      Configuration configuration = new Configuration();
      //2. Create an InputStream to read the data from local file
      InputStream inputStream = new BufferedInputStream(new FileInputStream("/home/client/localsystem/file/path/sample.txt"));
      //3. Get the HDFS instance
      FileSystem hdfs = FileSystem.get(new URI("hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:50070"), configuration);
      //4. Open a OutputStream to write the data, this can be obtained from the FileSytem
      OutputStream outputStream = hdfs.create(new Path("hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:50070/user/hadoop/Hadoop_File.txt"),
      new Progressable() {  
              @Override
              public void progress() {
         System.out.println("....");
              }
                    });
      try
      {
        IOUtils.copyBytes(inputStream, outputStream, 4096, false); 
      }
      finally
      {
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
      } 
  }
}