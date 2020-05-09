import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdp_conection {
	
	private static final String uri_hdfs = "hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:50070";
	
  public static void main(String[] args) throws IOException, URISyntaxException 
  {
    FileSystem fs = FileSystem.get(new URI(uri_hdfs), new Configuration());
    
    if(fs instanceof DistributedFileSystem) {
      System.out.println("-> HDFS is the underlying filesystem (" + fs.getClass() + ")" );
    }
    else {
      System.out.println("-> Other type of file system " + fs.getClass());
    }      
  }
}