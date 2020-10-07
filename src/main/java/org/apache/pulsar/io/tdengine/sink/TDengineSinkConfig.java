package org.apache.pulsar.io.tdengine.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class TDengineSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The url of the TDengine instance to connect to"
    )
    private String url;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The userName used to authenticate to TDengine"
    )
    private String userName;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The password used to authenticate to TDengine"
    )
    private String password;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The table Name is TDengine"
    )
    private String tableName;

    public static TDengineSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), TDengineSinkConfig.class);
    }

    public static TDengineSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), TDengineSinkConfig.class);
    }

    public void validate() {
        Preconditions.checkNotNull(url, "tdenginedbUrl property not set.");
    }
}
