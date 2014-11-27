package ca.sfu.dataming.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author congzicun
 * @since 2014-11-12 1:51 AM
 */
public class HDFSUtil {
    public static String getInputPaths(String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        FileStatus[] status_list = fs.listStatus(new Path(dirPath));
        StringBuilder sb = new StringBuilder();
        if (status_list != null) {
            for (FileStatus status : status_list) {
                //add each file to the list of inputs for the map-reduce job
                System.out.println(status.getPath());
                sb.append(",").append(status.getPath());
            }
        }
        String inputPath = sb.toString().replaceFirst(",", "");
        System.out.println(inputPath);
        return inputPath;
    }

    public static boolean delFile(String path) throws IOException {
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        return fs.delete(new Path(path), true);
    }

    public static long getFileSize(String path) throws IOException {
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        return fs.getFileStatus(new Path(path)).getLen();
    }

    public static InputStream getInputStream(String path) throws IOException {
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        return fs.open(new Path(path));
    }
}
