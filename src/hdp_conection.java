import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdp_conection {
	
	private static final String uri_hdfs = "hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:50070";
	
  public static void main(String[] args) throws IOException, URISyntaxException 
  {
//	  String localpath = "C://Users//tomsc/Desktop/test.txt";
//	  String hdfs_path = "hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:50070/tmp/data/WI17A_MES/";
	  
	  
    FileSystem fs = FileSystem.get(new URI(uri_hdfs), new Configuration());
    
    if(fs instanceof DistributedFileSystem) {
      System.out.println("-> HDFS is the underlying filesystem (" + fs.getClass() + ")" );
    }
    else {
      System.out.println("-> Other type of file system " + fs.getClass());
    } 
    
//    fs.copyFromLocalFile(new Path(localpath), new Path(hdfs_path));
    
  }
}