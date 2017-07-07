package repo.myjpa.elasticExport.util;

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
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;

/**
 * It can export a single lucene index
 */
public class LuceneIndexExporter {
    private static final byte[] LINE_ENDING = "\n".getBytes();

    private final Path indexPath;
    private final boolean ignoreDeleted;
    private final String exportFieldName;

    public LuceneIndexExporter(String path) throws IOException {
        this(path, true, "_source");
    }

    public LuceneIndexExporter(String path, boolean ignoreDeleted, String exportFieldName) throws IOException {
        this.indexPath = Paths.get(path);
        this.ignoreDeleted = ignoreDeleted;
        this.exportFieldName = exportFieldName;

    }

    public void export(OutputStream out) throws IOException {
        this.export(out, null);

    }

    /**
     * dump all documents in the Lucene Index to out.
     * You can call it multiple times to dump all documents multiple times
     *
     * @param out        output stream to write to
     * @param cbProgress a call back function will be called synchronously with
     *                   current progress (offset, size),
     *                   it's called for every 10,000 doc
     * @throws IOException
     */
    public void export(OutputStream out, ProgressReporter cbProgress) {
        Directory index = null;
        IndexReader reader = null;
        try {
            // initialize IndexReader
            index = FSDirectory.open(indexPath);
            reader = DirectoryReader.open(index);
            // get liveDoc bitmask to check if a docId is marked as deleted
            //https://lucene.apache.org/core/4_0_0/MIGRATE.html
            Bits liveDocs = MultiFields.getLiveDocs(reader);
            int maxDoc = reader.maxDoc();

            for (int i = 0; i < maxDoc; i++) {
                if (i % 10000 == 0) {
                    if (!cbProgress.report(i, maxDoc)) {
                        // call-back request to abort this process
                        return;
                    }
                }
                //TODO: should honor this.ignoreDeleted
                if (liveDocs == null || (liveDocs != null && liveDocs.get(i))) {
                    Document doc = reader.document(i);
                    out.write(doc.getField(exportFieldName).binaryValue().bytes);
                    out.write(LINE_ENDING);
                }
            }
        } catch (Exception e) {

        } finally {
            try {
                if (reader != null) reader.close();
                if (index != null) index.close();
            } catch (IOException ioe) {
                // TODO: log failed to close LuceneIndexReder
            }
        }

    }

}
