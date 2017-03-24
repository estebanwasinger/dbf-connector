package org.mule.modules.dbfreader.config;

import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.display.Path;
import org.mule.api.annotations.display.Summary;
import org.mule.api.annotations.param.Optional;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {

    @Configurable
    @Path
    @Summary("The path where the DBF File are placed")
    private String folder;

    @Configurable
    @Optional
    @Summary("The charset to use to read the String values")
    private String charset;

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }


    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

}