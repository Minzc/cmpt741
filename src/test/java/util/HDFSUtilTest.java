package util;

import ca.sfu.dataming.util.HDFSUtil;
import org.junit.Test;

import java.io.IOException;

/**
 * @author congzicun
 * @since 2014-11-20 4:31 PM
 */
public class HDFSUtilTest {
    @Test
    public void testFileSize() throws IOException {
        String path = "data_100k_t10_p4_l2000_n1k.data";
        long rst = HDFSUtil.getFileSize(path);
        System.out.println(rst);
    }
}
