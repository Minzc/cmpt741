package util;

import ca.sfu.dataming.util.StringUtil;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author congzicun
 * @since 2014-11-11 10:12 AM
 */
public class StringUtilTest {
    @Test
    public void testSplit() {

        System.out.println("#1".split("#").length);
    }
    @Test
    public void testReplace(){

        System.out.println("|abc".replaceFirst(StringUtil.STR_DELIMIT_1ST,""));
    }
    @Test
    public void testRetain(){
        Set<String> a = new HashSet<String>();
        Set<String> b = new HashSet<String>();
        a.add("a");
        a.add("b");
        a.add("c");
        b.add("a");
        b.add("b");
        b.retainAll(a);
        System.out.println(b.size());
    }

    @Test
    public void testMerge(){
        int[] array = {1,2,3};
        System.out.println(StringUtil.mergeArray(ArrayUtils.toObject(array), StringUtil.DELIMIT_1ST));
    }
}
