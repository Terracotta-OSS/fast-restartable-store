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
      KeyGenerator keyGenerator;
      try {
        keyGenerator = KeyGenerator.getInstance("AES");
      } catch (NoSuchAlgorithmException ex) {
        throw new RuntimeException("test failed due to missing cipher algorithm", ex);
      }
      keyGenerator.init(256);
      String cipherKeyStr = Base64.getEncoder().encodeToString(keyGenerator.generateKey().getEncoded());
      props.setProperty(FrsProperty.STORE_ENCRYPTION_ENABLE.shortName(), "true");
      props.setProperty(FrsProperty.STORE_ENCRYPTION_KEY.shortName(), cipherKeyStr);
      props.setProperty(FrsProperty.STORE_ENCRYPTION_ALGORITHM.shortName(), "AES/CFB/PKCS5Padding");
    } else {
      props.setProperty(FrsProperty.STORE_ENCRYPTION_ENABLE.shortName(), "false");
    }
    return props;
  }
}
