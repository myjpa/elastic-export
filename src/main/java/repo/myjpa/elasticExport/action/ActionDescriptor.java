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

import repo.myjpa.elasticExport.util.NullOutputStream;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * Describe an Action
 * Created by haoliu on 7/7/2017.
 */
public class ActionDescriptor {
    private String action;
    private String srcFolder;
    private String destFolder;
    private String onlyPrimary;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    String getActionClassName() {
        if(action==null) return null;
        String capitalizedAction = action.substring(0, 1).toUpperCase() + action.substring(1);
        return Action.class.getPackage().getName() + "." + capitalizedAction + "Action";
    }

    public String getSrcFolder() {
        return srcFolder;
    }

    public void setSrcFolder(String srcFolder) {
        this.srcFolder = srcFolder;
    }

    public String getDestFolder() {
        return destFolder;
    }

    public void setDestFolder(String destFolder) {
        this.destFolder = destFolder;
    }

    /**
     * get the output stream to write to
     * @param filename output file name without extension to be put into destFolder
     * @return the output stream according to ActionDescriptor settings
     */
    OutputStream getOutputStream(String filename) throws IOException {
        OutputStream ret = null;
        switch(this.destFolder){
            case "STDOUT":
                ret = System.out;
                break;
            case "NULL":
                ret = NullOutputStream.INSTANCE;
                break;
            default:
                String path = this.destFolder+"/"+filename+".json.gz";
                ret = new GZIPOutputStream(new FileOutputStream(new File(path)));
                break;
        }
        return ret;

    }

    public String getOnlyPrimary() {
        return onlyPrimary;
    }

    public void setOnlyPrimary(String onlyPrimary) {
        this.onlyPrimary = onlyPrimary;
    }
}
