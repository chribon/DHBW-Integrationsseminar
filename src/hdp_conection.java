import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class hdp_conection {
	
	// path to Hadoop-System
	private static final String NAME_NODE = "hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:8080";
	
  public static void main(String[] args) throws IOException, URISyntaxException 
  {
	  // connect to filesystem
    FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());
    
    // check weather the underlying filesystem
    // https://www.folkstalk.com/2013/06/connect-to-hadoop-hdfs-through-java.html
    
    if(fs instanceof DistributedFileSystem) {
      System.out.println("-> HDFS is the underlying filesystem");
    }
    else {
      System.out.println("-> Other type of file system "+fs.getClass());
    }
    
//    String fileInHdfs = args[0];
//    String fileContent = IOUtils.toString(fs.open(new Path(fileInHdfs)), "UTF-8");
//    System.out.println("File content - " + fileContent);
    
    
//    try{
//
//        FileStatus[] status = fs.listStatus(new Path("hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:8080/tmp/data/trucks"));  // you need to pass in your hdfs path
//
//        for (int i=0;i<status.length;i++){
//            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
//            String line;
//            line=br.readLine();
//            while (line != null){
//                System.out.println(line);
//                line=br.readLine();
//            }
//        }
//    }catch(Exception e){
//        System.out.println("File not found");
//    }
    
    
    
  }
}



//
//public class hdp_conection {
//
//  private static final String NAME_NODE = "hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:8080/";
//
//  public static void main(String[] args) throws URISyntaxException, IOException {
//      String fileInHdfs = args[0];
//      FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());
//      String fileContent = IOUtils.toString(fs.open(new Path(fileInHdfs)), "UTF-8");
//      System.out.println("File content - " + fileContent);
//  }
//
//}