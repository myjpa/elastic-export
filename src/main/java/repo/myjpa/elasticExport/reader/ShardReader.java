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

package repo.myjpa.elasticExport.reader;

import com.fasterxml.jackson.databind.JsonNode;
import repo.myjpa.elasticExport.util.LuceneIndexExporter;
import repo.myjpa.elasticExport.util.MetadataFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * read an ElasticSearch shard stored in a file-system directory(path=indexDir/shardId)
 * Created by haoliu on 7/6/2017.
 */
public class ShardReader {
    private final String STATE_FOLDER = "/_state";
    private final String DATA_FOLDER = "/index";

    private JsonNode metadata;

    private String indexName;
    private String indexDir;
    private String shardId;


    public boolean open(String indexDir, String shardId) {
        try {
            this.indexDir = indexDir;
            this.shardId = shardId;
            readStateFile(this.getShardDir());

        } catch (Exception e) {
            // TODO: logging cannot read metadata
        }
        return this.isValid();
    }

    public LuceneIndexExporter getExporter() throws IOException {
        LuceneIndexExporter exporter = new LuceneIndexExporter(this.getShardDir() + DATA_FOLDER);
        return exporter;
    }

    private void readStateFile(final String baseDir) throws IOException {
        metadata = MetadataFileUtil.readMetadataFileInFolder(baseDir + STATE_FOLDER);
    }

    private String getShardDir() {
        return indexDir + "/" + shardId;
    }


    public boolean isValid() {
        return metadata != null;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getShardId() {
        return shardId;
    }

    public boolean isPrimary() {
        return metadata.get("primary").asBoolean();
    }
}
