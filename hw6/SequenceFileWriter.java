package stubs;

import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.io.Text;

public class SequenceFileWriter
{
  public static void main(String[] paramArrayOfString)
    throws IOException
  {
    if (paramArrayOfString.length != 2)
    {
      System.out.printf("Usage: SequenceFileWriter <input dir> <output dir>\n", new Object[0]);
      return;
    }
    Configuration localConfiguration = new Configuration();
    
    Path localPath = new Path(paramArrayOfString[1]);
    Text localText1 = new Text();
    Text localText2 = new Text();
    SequenceFile.Writer localWriter = null;
    BufferedReader localBufferedReader = new BufferedReader(new FileReader(paramArrayOfString[0]));
    try
    {
      localWriter = SequenceFile.createWriter(localConfiguration, new SequenceFile.Writer.Option[] { SequenceFile.Writer.file(localPath), SequenceFile.Writer.keyClass(localText1.getClass()), SequenceFile.Writer.valueClass(localText2.getClass()) });
      String str1 = localBufferedReader.readLine();
      while (str1 != null)
      {
        String str2 = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
        Pattern localPattern = Pattern.compile(str2);
        Matcher localMatcher = localPattern.matcher(str1);
        if (localMatcher.find())
        {
          localText1.set(localMatcher.group());
          localText2.set(str1);
          
          System.out.printf("[%s]\t%s\t%s\n", new Object[] { Long.valueOf(localWriter.getLength()), localText1, localText2 });
          localWriter.append(localText1, localText2);
        }
        str1 = localBufferedReader.readLine();
      }
    }
    finally
    {
      IOUtils.closeStream(localWriter);
      localBufferedReader.close();
    }
  }
}