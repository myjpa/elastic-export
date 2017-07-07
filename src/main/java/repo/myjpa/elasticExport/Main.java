package repo.myjpa.elasticExport;


import com.fasterxml.jackson.databind.JsonNode;
import repo.myjpa.elasticExport.reader.IndexReader;
import repo.myjpa.elasticExport.reader.ShardReader;
import repo.myjpa.elasticExport.util.MetadataFileUtil;
import repo.myjpa.elasticExport.util.NullOutputStream;

/**
 * testing runs for now, hooked up with testing folder not check-ed in
 * Created by haoliu on 6/28/2017.
 */
public class Main {
    public static void main(String[] argv) throws Exception {


         String stateFile;
        // read a state file in binary SMILE format
        // Z:\tmp\clue\clue\example-statefile\1.st
        //String stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\1.st";
        // snapshot shard description file
        //String stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\index-27";
        // shard state
        stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\state-92.st";
        // index state
        stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\state-index.st";
        // cluster state
        //stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\global-1605.st";

        JsonNode data = MetadataFileUtil.readFile(stateFile);
        System.out.println(data.toString());

        String indexBaseDir = "Z:\\tmp\\clue\\clue\\example-index";
        IndexReader ir = new IndexReader();
        ir.open(indexBaseDir);
        for (ShardReader s : ir.getShards()) {
            s.getExporter().export(
                    NullOutputStream.INSTANCE,
                    (int offset, int size) -> {
                        System.out.println(offset + "/" + size);
                        return true;
                    }
            );
        }
    }
}
