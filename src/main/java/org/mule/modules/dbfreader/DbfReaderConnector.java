package org.mule.modules.dbfreader;

import static org.mule.api.annotations.param.MetaDataKeyParamAffectsType.OUTPUT;
import org.mule.modules.jdbf.core.DbfField;
import org.mule.modules.jdbf.core.DbfFieldTypeEnum;
import org.mule.modules.jdbf.core.DbfMetadata;
import org.mule.modules.jdbf.core.DbfRecord;
import org.mule.modules.jdbf.reader.DbfReader;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.MetaDataScope;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.display.UserDefinedMetaData;
import org.mule.api.annotations.param.MetaDataKeyParam;
import org.mule.api.transformer.DataType;
import org.mule.devkit.api.transformer.DefaultTranformingValue;
import org.mule.devkit.api.transformer.TransformingValue;
import org.mule.modules.dbfreader.config.ConnectorConfig;
import org.mule.transformer.types.DataTypeFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Connector(name="dbf-reader", friendlyName="DBF")
@MetaDataScope( DataSenseResolver.class )
public class DbfReaderConnector {

    private static final DataType DATA_TYPE = DataTypeFactory.create(List.class, Map.class, "application/java");

    @Config
    private ConnectorConfig config;

    @Processor(friendlyName = "Read DBF")
    @UserDefinedMetaData
    public TransformingValue<List<Map<String,Object>>, DataType<List<Map<String,Object>>>> readDbf(@MetaDataKeyParam(affects = OUTPUT) String fileName) throws IOException {
        DbfReader dbfReader = new DbfReader(new File(config.getFolder(), fileName));
        return new DefaultTranformingValue<>(dbfToListMap(dbfReader, getCharset()), DATA_TYPE);
    }

    private Charset getCharset() {
        Charset charset;
        try {
            charset = Charset.forName(config.getCharset());
        } catch (UnsupportedCharsetException e){
            System.out.println("The selected charset is invalid");
            charset = Charset.defaultCharset();
        }
        return charset;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }

    private List<Map<String, Object>> dbfToListMap(DbfReader dbfReader, Charset charset) throws IOException {
        DbfMetadata metadata = dbfReader.getMetadata();

        DbfRecord read = dbfReader.read();
        List<Map<String, Object>> mapList = new ArrayList<>();
        while (read != null){
            Map<String, Object> map = new HashMap<>();

            DbfRecord finalRead = read;
            for (DbfField field : metadata.getFields()) {
                String fieldName = field.getName();
                DbfFieldTypeEnum type = metadata.getField(fieldName).getType();
                Object fieldValue;
                if (type.equals(DbfFieldTypeEnum.Date)){
                    try {
                        fieldValue = finalRead.getDate(fieldName);
                    } catch (ParseException e) {
                        fieldValue = finalRead.getString(fieldName, charset);
                    }
                } else {
                    fieldValue = finalRead.getString(fieldName, charset);
                }

                map.put(fieldName, fieldValue);
            }

            mapList.add(map);
            read = dbfReader.read();
        }
        return mapList;
    }

}