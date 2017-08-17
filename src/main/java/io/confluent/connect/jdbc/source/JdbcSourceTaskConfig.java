/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTaskConfig.class);

  public static final String TABLES_CONFIG = "tables";
  private static final String TABLES_DOC = "List of tables for this task to watch for changes.";


  //static ConfigDef config = baseConfigDef()
      //.define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

  public JdbcSourceTaskConfig(Map<String, String> props) {

    super(modifyConfig(props), props);
  }

  private static ConfigDef modifyConfig(Map<String,String >  props){

    ConfigDef config = baseConfigDef()
            .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

    for(String prop:props.keySet()){

      if(prop.endsWith("." + INCREMENTING_COLUMN_NAME_CONFIG)){
        config.define(prop, Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,2, ConfigDef.Width.MEDIUM,"");
      }
      else if(prop.endsWith("."+TIMESTAMP_COLUMN_NAME_CONFIG)){
        config.define(prop, Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
      else if(prop.endsWith("."+MODE_CONFIG)){
        config.define(prop, Type.STRING, MODE_UNSPECIFIED, ConfigDef.ValidString.in(MODE_UNSPECIFIED, MODE_BULK, MODE_TIMESTAMP, MODE_INCREMENTING, MODE_TIMESTAMP_INCREMENTING),
                Importance.HIGH, "Doc", MODE_GROUP, 1, ConfigDef.Width.MEDIUM, "", Arrays.asList(INCREMENTING_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME_CONFIG, VALIDATE_NON_NULL_CONFIG));
      }
      else if ((prop.equals(ANONYMIZE+".default")) || (prop.endsWith(ANONYMIZE + ".column.name"))) {
        config.define(prop,Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
      else if ((prop.endsWith(".whitelist.column.name"))) {
        config.define(prop,Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
      else if ((prop.equals("anonymization.keystore.pass"))) {
        config.define(prop,Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
      else if ((prop.equals("anonymization.keystore.path"))) {
        config.define(prop,Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
    }

    return config;
  }
}
