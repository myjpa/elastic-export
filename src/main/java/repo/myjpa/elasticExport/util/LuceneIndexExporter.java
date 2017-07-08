/*
 *  MIT License
 *
 *  Copyright (c) 2017 Hao Liu (https://github.com/myjpa)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

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
     * @param onProgress a call back function will be called synchronously with
     *                   current progress (offset, size),
     *                   it's called for every 10,000 doc
     * @throws IOException
     */
    public void export(OutputStream out, OnProgress onProgress) {
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
                if (onProgress != null && i % 10000 == 0) {
                    if (!onProgress.report(i, maxDoc)) {
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
