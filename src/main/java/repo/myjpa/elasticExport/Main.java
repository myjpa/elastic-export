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

package repo.myjpa.elasticExport;


import com.fasterxml.jackson.databind.JsonNode;
import repo.myjpa.elasticExport.frontend.CommandLineRouter;
import repo.myjpa.elasticExport.reader.IndexReader;
import repo.myjpa.elasticExport.reader.ShardReader;
import repo.myjpa.elasticExport.util.MetadataFileUtil;
import repo.myjpa.elasticExport.util.NullOutputStream;

/**
 * testing runs for now, hooked up with testing folder not check-ed in
 * Created by haoliu on 6/28/2017.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        CommandLineRouter.run(args);

    }
    public static void backup_main(String[] args) throws Exception {


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
        stateFile = "Z:\\tmp\\clue\\clue\\example-statefile\\global-1605.st";

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
