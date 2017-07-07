package repo.myjpa.elasticExport.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * This reads an ElasticSearch meta-data file in JSON/SMILE format
 * Created by haoliu on 7/5/2017.
 */
public class MetadataFileUtil {
    // elasticsearch first 18 bytes encodes
    // 0~4 org.apache.lucene.codecs.CodecUtil.CODEC_MAGIC = 0x3fd76c17
    // 5~18 length and file-type signature, like state, index-metadata
    private static final long SMILE_FILE_HEADER = 18L;
    private static final int FILE_HEADER_LENGTH = 20;

    public static JsonNode readFile(String path) {
        FileInputStream input = null;
        JsonNode ret = null;
        try {
            input = new FileInputStream(path);
            ObjectMapper mapper = createObjectMapper(input);
            ret = mapper.readValue(input, JsonNode.class);
            input.close();
        } catch (Exception e) {
            // throw a descriptive exception here
        } finally {
            return ret;
        }
    }

    /**
     * read first file in the baseDir and return parsed JsonNode
     * @param baseDir
     * @return JsonNode of the metadata file or null if cannot find/read the file
     * @throws IOException
     */
    public static JsonNode readMetadataFileInFolder(final String baseDir) throws IOException {
        File[] files = new File(baseDir).listFiles();
        if(files.length == 0) {
            return null;
        }
        // TODO: allow user to provide a glob filter for file names
        File stateFile = files[0];
        JsonNode ret = readFile(stateFile.getPath());
        return ret;
    }

    private Path getStateFolder(final String baseDir) {
        return Paths.get(baseDir, "_state");
    }

    /**
     * create ObjectMapper and seek input to proper position to readValue
     *
     * @param input
     * @return
     * @throws IOException
     */
    private static ObjectMapper createObjectMapper(FileInputStream input) throws IOException {
        ObjectMapper mapper = null;

        byte[] header = new byte[FILE_HEADER_LENGTH];
        int readSize = input.read(header, 0, FILE_HEADER_LENGTH);

        if (readSize <= 0) {
            // cannot read meaningful header to probe
            throw new IOException("cannot read sufficent data to probe file format");
        } else {
            // read something so that we can detect file type
            if (header[0] == (byte) '{' && header[1] == (byte) '"') {
                // JSON
                mapper = new ObjectMapper();
                input.skip(0 - FILE_HEADER_LENGTH);
            } else if (header[0] == (byte) 0x3f && header[1] == (byte) 0xd7 && header[2] == (byte) 0x6c && header[3] == (byte) 0x17) {
                // we are seeing Lucene CODEC_MAGIC here, continue to probe
                if (header[18] == 0x3a && header[19] == 0x29) {
                    // smile format
                    mapper = new ObjectMapper(new SmileFactory());
                    input.skip(SMILE_FILE_HEADER - FILE_HEADER_LENGTH);
                } else {
                    // unrecognized Lucene codec file
                }
            }
        }

        return mapper;

    }
/*
    HashMap<String, Object> data;

    public MetadataFileUtil(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new SmileFactory());
        FileInputStream input = new FileInputStream(path);
        input.skip(SMILE_FILE_HEADER);
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<HashMap<String, Object>>() {
        };

        data = mapper.readValue(input, typeRef);
    }

    public HashMap<String, Object> getData() {
        return data;
    }
    */
}
