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
package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;

import java.util.Optional;

public class NoEncryptionHandler implements EncryptionHandler {

  @Override
  public String getCurrToken() {
    throw new UnsupportedOperationException("operation unsupported");
  }

  @Override
  public Optional<String> getPreviousToken() {
    return Optional.empty();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return false;
  }

  @Override
  public void add(String token, byte[] key) {
    throw new UnsupportedOperationException("operation unsupported");
  }

  @Override
  public void remove(String token) {
    throw new UnsupportedOperationException("operation unsupported");
  }

  @Override
  public Action convert(Action action) {
    return action;
  }
}
