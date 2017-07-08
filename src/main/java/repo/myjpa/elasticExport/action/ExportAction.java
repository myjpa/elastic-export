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

package repo.myjpa.elasticExport.action;

import repo.myjpa.elasticExport.reader.IndexReader;
import repo.myjpa.elasticExport.reader.ShardReader;
import repo.myjpa.elasticExport.util.NullOutputStream;
import repo.myjpa.elasticExport.util.OnProgress;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * export an ES folder
 * Created by haoliu on 7/7/2017.
 */
public class ExportAction extends Action {
    @Override
    public void setup(ActionDescriptor ad) throws IllegalArgumentException {
        super.setup(ad);
        validate(!ad.getSrcFolder().isEmpty(), "require source folder");
        validate(!ad.getDestFolder().isEmpty(), "require dest folder");
    }

    @Override
    public void run(OnProgress onProgress) throws Exception {
        IndexReader ir = new IndexReader();
        ir.open(ad.getSrcFolder());
        for (ShardReader sr : ir.getShards()) {
            OutputStream out = ad.getOutputStream(sr.getIndexName()+"."+sr.getShardId());
            sr.getExporter().export(out, onProgress);
        }
    }

}
