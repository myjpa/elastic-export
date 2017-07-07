package repo.myjpa.elasticExport.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * null output stream
 * borrowed from here: https://stackoverflow.com/a/692580
 * Created by haoliu on 7/7/2017.
 */
public class NullOutputStream extends OutputStream {
    public static final NullOutputStream INSTANCE = new NullOutputStream();
    @Override
    public void write(int b) throws IOException {
    }
}