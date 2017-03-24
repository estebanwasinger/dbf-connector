package org.mule.modules.dbfreader;

import org.mule.modules.jdbf.core.DbfField;
import org.mule.modules.jdbf.core.DbfMetadata;
import org.mule.modules.jdbf.reader.DbfReader;
import org.mule.api.annotations.MetaDataKeyRetriever;
import org.mule.api.annotations.MetaDataRetriever;
import org.mule.api.annotations.components.MetaDataCategory;
import org.mule.common.metadata.DefaultMetaData;
import org.mule.common.metadata.DefaultMetaDataKey;
import org.mule.common.metadata.MetaData;
import org.mule.common.metadata.MetaDataKey;
import org.mule.common.metadata.MetaDataModel;
import org.mule.common.metadata.builder.DefaultMetaDataBuilder;
import org.mule.common.metadata.builder.DynamicObjectBuilder;
import org.mule.common.metadata.datatype.DataType;
import org.mule.common.metadata.datatype.DataTypeFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FilenameFilter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Category which can differentiate between input or output MetaDataRetriever
 */
@MetaDataCategory
public class DataSenseResolver {

    /**
     * If you have a service that describes the entities, you may want to use
     * that through the connector. Devkit will inject the connector, after
     * initializing it.
     */
    @Inject
    private DbfReaderConnector connector;

    /**
     * Retrieves the list of keys
     */
    @MetaDataKeyRetriever
    public List<MetaDataKey> getMetaDataKeys() throws Exception {
        String folder = connector.getConfig().getFolder();
        File file = new File(folder);
        File[] dbfs = file.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith("dbf");
            }
        });

        List<MetaDataKey> metaDataKeys = new ArrayList<>();

        if(dbfs != null){
            for (File dbf : dbfs) {
                metaDataKeys.add(new DefaultMetaDataKey(dbf.getName(), dbf.getName()));
            }
        }

        return metaDataKeys;
    }

    /**
     * Get MetaData given the Key the user selects
     * 
     * @param key The key selected from the list of valid keys
     * @return The MetaData model of that corresponds to the key
     * @throws Exception If anything fails
     */
    @MetaDataRetriever
    public MetaData getMetaData(MetaDataKey key) throws Exception {
        String id = key.getId();

        DefaultMetaDataBuilder builder = new DefaultMetaDataBuilder();
        DynamicObjectBuilder<?> dynamicObject = builder.createDynamicObject(id);
        if(!id.startsWith("#[")){
            String folder = connector.getConfig().getFolder();
            File dbfFile = new File(folder, id);

            DbfReader dbfReader = new DbfReader(dbfFile);
            DbfMetadata metadata = dbfReader.getMetadata();

            for (DbfField dbfField : metadata.getFields()) {
                String fieldName = dbfField.getName();

                switch (dbfField.getType()) {
                    case Date:
                        dynamicObject.addSimpleField(fieldName, DataType.DATE);
                        break;
                    case Numeric:
                        dynamicObject.addSimpleField(fieldName, DataType.NUMBER);
                        break;
                    case Character:
                        dynamicObject.addSimpleField(fieldName, DataType.STRING);
                        break;
                    case Logical:
                        dynamicObject.addSimpleField(fieldName, DataType.BOOLEAN);
                        break;
                    case Double:
                        dynamicObject.addSimpleField(fieldName, DataType.DOUBLE);
                        break;
                    case Integer:
                        dynamicObject.addSimpleField(fieldName, DataType.INTEGER);
                        break;
                    case Float:
                        dynamicObject.addSimpleField(fieldName, DataType.FLOAT);
                        break;
                    case Double7:
                        dynamicObject.addSimpleField(fieldName, DataType.DOUBLE);
                        break;
                    case DateTime:
                        dynamicObject.addSimpleField(fieldName, DataType.DATE_TIME);
                        break;
                    case Currency:
                        dynamicObject.addSimpleField(fieldName, DataTypeFactory.getInstance().getDataType(BigDecimal.class));
                        break;
                    default:
                        dynamicObject.addSimpleField(fieldName, DataType.STRING);
                }
            }
        }

        MetaDataModel model = builder.build();
        return new DefaultMetaData(model);
    }

    public DbfReaderConnector getConnector() {
        return connector;
    }

    public void setConnector(DbfReaderConnector connector) {
        this.connector = connector;
    }
}
