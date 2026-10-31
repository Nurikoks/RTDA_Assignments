package aitu;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class NoPermLocalFs extends DelegateToFileSystem {
    public NoPermLocalFs(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(uri, new NoPermLocalFileSystem(), conf, "file", false);
    }
}
