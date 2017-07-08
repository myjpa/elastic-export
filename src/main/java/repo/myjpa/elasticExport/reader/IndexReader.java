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
import repo.myjpa.elasticExport.util.MetadataFileUtil;

import java.io.IOException;
import java.util.*;

/**
 * read an ElasticSearch index stored in a file-system directory
 *
 * Created by haoliu on 7/6/2017.
 */

public class IndexReader {
    private final String STATE_FOLDER = "/_state";

    private String indexDir;
    // index meta-data
    private String indexName;
    private String state;
    private String uuid;
    private int totalShardCount;
    // shard reader
    private List<ShardReader> shards;


    public void open(String baseDir) throws Exception {
        this.indexDir = baseDir;
        readStateFile(baseDir);
        createShardReaders();
    }

    public List<ShardReader> getShards() {
        return Collections.unmodifiableList(shards);
    }


    private void readStateFile(final String baseDir) throws IOException {
        JsonNode meta = MetadataFileUtil.readMetadataFileInFolder(baseDir+STATE_FOLDER);
        Map.Entry<String,JsonNode> indexEntry = meta.fields().next();

        indexName = indexEntry.getKey();
        state = indexEntry.getValue().get("state").asText();
        uuid = indexEntry.getValue().get("settings").get("index.uuid").asText();
        totalShardCount = indexEntry.getValue().get("routing_num_shards").asInt();

    }

    private void createShardReaders() throws Exception {
        shards = new ArrayList<>(totalShardCount);
        for(int i = 0; i< totalShardCount; i++) {
            ShardReader sr = new ShardReader();
            sr.setIndexName(indexName);
            // shard is stored in {index_dir}/{shard_id}
            if(sr.open(this.indexDir, i+"")) {
                shards.add(sr);
            }
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public String getState() {
        return state;
    }

    public String getUuid() {
        return uuid;
    }

    public int getTotalShardCount() {
        return totalShardCount;
    }
}
