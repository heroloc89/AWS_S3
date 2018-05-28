import net.lingala.zip4j.model.FileHeader;

import java.io.*;
import java.nio.file.Paths;
import java.util.zip.ZipFile;

/**
 * Created by LOCNGUYEN on 5/28/2018.
 */
public class FileTransfer {

    /**
     * Shovels all data from an input stream to an output stream.
     */
    protected void shovelInToOut(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
    }

    protected void safeCloseResouce(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    public enum CompressionType {
        GZIP(1),
        ZIP(2);
        private int value;
        CompressionType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

}
