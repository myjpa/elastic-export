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
