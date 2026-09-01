/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs;

import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Properties;

import javax.crypto.KeyGenerator;

import com.terracottatech.frs.config.FrsProperty;

public class CipherHelper {

  public static Properties configure(boolean encryptLog, Properties props) {
    if (encryptLog) {
      props.setProperty(FrsProperty.STORE_ENCRYPTION_ENABLE.shortName(), "true");
      props.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN.shortName(), "token1");
      props.setProperty(FrsProperty.STORE_ENCRYPTION_NEW_KEY.shortName(), generateNewKey());
    } else {
      props.setProperty(FrsProperty.STORE_ENCRYPTION_ENABLE.shortName(), "false");
    }
    return props;
  }

  public static String generateNewKey() {
    KeyGenerator keyGenerator;
    try {
      keyGenerator = KeyGenerator.getInstance("AES");
    }  catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("test failed due to missing cipher algorithm", e);
    }
    keyGenerator.init(256);
    return Base64.getEncoder().encodeToString(keyGenerator.generateKey().getEncoded());
  }
}
