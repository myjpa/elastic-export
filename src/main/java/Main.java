import repo.myjpa.elasticExport.IndexExporter;

import java.io.IOException;

/**
 * Created by haoliu on 6/28/2017.
 */
public class Main {
    public static void main(String[] argv) throws IOException {

        System.out.println("start");
        IndexExporter exporter = new IndexExporter(argv[0]);
        exporter.export(System.out);

    }
}
