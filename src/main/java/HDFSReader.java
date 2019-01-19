import java.io.InputStream;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import java.nio.charset.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileStatus;

public class HDFSReader {
    public static String readFile(Configuration conf, Path path) throws IOException {
    
    FileSystem fs=FileSystem.get(conf);
    String text = "";

    FileStatus fStatus = fs.getFileStatus(path);

    if (fStatus.isDirectory()) {
      RemoteIterator<LocatedFileStatus> fList = fs.listFiles(path, false);
      while(fList.hasNext()) {
        InputStream cFile = fs.open(fList.next().getPath());
        text += new String(IOUtils.toByteArray(cFile), StandardCharsets.UTF_8);
      }
    } else {
      InputStream is=fs.open(path);
      text = new String(IOUtils.toByteArray(is), StandardCharsets.UTF_8);
    }
    
    return text;
  }
}