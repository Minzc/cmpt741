import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author congzicun
 * @since 2014-11-02 2:49 PM
 */
public class HelloWorld {
    static Log LOG = LogFactory.getLog(HelloWorld.class);
    public static void main(String[] args) {
        System.out.println("Hello World");
        LOG.info("Hello World");
    }
}
