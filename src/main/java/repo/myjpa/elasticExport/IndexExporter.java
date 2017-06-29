package repo.myjpa.elasticExport;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Agent that can export a lucene index with various configuration
 */
public class IndexExporter {
    private static final byte[] LINE_ENDING = "\n".getBytes();

    private final Path indexPath;
    private final boolean ignoreDeleted;
    private final String exportFieldName;

    private IndexReader reader;

    public IndexExporter(String path) throws IOException {
        this(path, true, "_source");
    }

    public IndexExporter(String path, boolean ignoreDeleted, String exportFieldName) throws IOException {
        this.indexPath = Paths.get(path);
        this.ignoreDeleted = ignoreDeleted;
        this.exportFieldName = exportFieldName;
        // initialize IndexReader
        Directory index = FSDirectory.open(indexPath);
        this.reader = DirectoryReader.open(index);
    }

    public void export(OutputStream out) throws IOException {
        // get liveDoc bitmask to check if a docId is marked as deleted
        //https://lucene.apache.org/core/4_0_0/MIGRATE.html
        Bits liveDocs = MultiFields.getLiveDocs(reader);

        for (int i=0; i<reader.maxDoc(); i++) {
            //TODO: should honor this.ignoreDeleted
            if (liveDocs == null || (liveDocs!=null && liveDocs.get(i))) {
                Document doc = reader.document(i);
                out.write(doc.getField(exportFieldName).binaryValue().bytes);
                out.write(LINE_ENDING);
            }
        }

    }
}
