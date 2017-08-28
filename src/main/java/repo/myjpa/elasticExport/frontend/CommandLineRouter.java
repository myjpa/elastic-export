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

package repo.myjpa.elasticExport.frontend;

import org.apache.commons.cli.*;
import repo.myjpa.elasticExport.action.Action;
import repo.myjpa.elasticExport.action.ActionDescriptor;

import java.util.List;

/**
 * route a command-line invocation to an action
 * Created by haoliu on 7/7/2017.
 */
public class CommandLineRouter {


    private static final Options options;
    private CommandLine cmdline;

    // define options
    static {
        options = new Options();

        options.addOption("a", "action", true,
                "Action, the only choice is export(default) for now");
        options.addOption("s", "src", true,
                "Source directory: ElasticSearch index data folder. eg. data.repo/some_cluster/nodes/0/indices/index-uuid, default to current directory");
        options.addOption("d", "dest", true,
                "Destination directory to export to, with file name as ${index}.${shardId}.json.gz; You can use STDOUT (this is default) to output to standard output or NULL to discard the output");
        options.addOption("p", "only-primary", false,
                "only export primary shard");
    }

    private CommandLineRouter(CommandLine cmdline) {
        this.cmdline = cmdline;
        route();
    }

    public static CommandLineRouter run(String[] args) {

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmdline = parser.parse(options, args);
            return new CommandLineRouter(cmdline);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            return null;
        }
    }

    private void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("elastic-export", options);
    }

    /**
     * route the request
     */
    private void route() {
        ActionDescriptor ad = new ActionDescriptor();
        ad.setAction(cmdline.getOptionValue("a", "export"));
        ad.setSrcFolder(cmdline.getOptionValue("s","."));
        ad.setDestFolder(cmdline.getOptionValue("d","STDOUT"));
        ad.setOnlyPrimary(Boolean.parseBoolean(cmdline.getOptionValue("p", "true")));

        Action action = Action.create(ad);
        if (action == null) {
            printUsage();

        } else {
            try {
                action.run((offset, size) -> {
                    System.err.println(offset + "/" + size);
                    return true;
                });
            } catch (Exception e) {
                System.err.println(e.toString());

            }
        }


    }
}
