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

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author prasanta
 */
public class SecurePutAction extends PutAction {
  /*
   * CipheredPutAction Header
   * 4 bytes - idByteCount
   * 4 bytes - keyByteCount
   * 4 bytes - ivByteCount
   * 4 bytes - valueByteCount
   * 8 bytes - invalidatedLsn
   */
  public static final long CIPHERED_PUT_ACTION_OVERHEAD = 24L;

  public static class SecureActionFactory implements ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final CipherManager cipherManager;

    public SecureActionFactory(CipherManager cipherManager) {
      this.cipherManager = cipherManager;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
        ActionCodec codec, ByteBuffer[] buffers) {
      int idLength = ByteBufferUtils.getInt(buffers);
      int keyLength = ByteBufferUtils.getInt(buffers);
      int ivLength = ByteBufferUtils.getInt(buffers);
      int valueLength = ByteBufferUtils.getInt(buffers);
      long invalidatedLsn = ByteBufferUtils.getLong(buffers);
      ByteBuffer id = ByteBufferUtils.getBytes(idLength, buffers);
      ByteBuffer key = ByteBufferUtils.getBytes(keyLength, buffers);
      ByteBuffer iv = ByteBufferUtils.getBytes(ivLength, buffers);
      ByteBuffer cipheredValue = ByteBufferUtils.getBytes(valueLength, buffers);
      ByteBuffer value = cipherManager.decrypt(cipheredValue, iv);
      return new SecurePutAction(objectManager, null, cipherManager, id, key, iv, value, invalidatedLsn);
    }
  }

  private static final int HEADER_SIZE = ByteBufferUtils.INT_SIZE * 4 + ByteBufferUtils.LONG_SIZE;

  // buffer contains initialization vector which introduces randomness and
  // ensure same plaintext encrypts to different ciphertexts each time
  private final ByteBuffer iv;
  private final CipherManager cipherManager;

  SecurePutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      Compactor compactor, CipherManager cipherManager, ByteBuffer id, ByteBuffer key,
      ByteBuffer value, boolean recovery) {
    super(objectManager, compactor, id, key, value, recovery);
    this.iv = cipherManager.generateInitializationVector();
    this.cipherManager = cipherManager;
  }

  public SecurePutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      Compactor compactor, CipherManager cipherManager, ByteBuffer id, ByteBuffer key,
      ByteBuffer iv, ByteBuffer value, long invalidatedLsn) {
    super(objectManager, compactor, id, key, value, invalidatedLsn);
    this.iv = iv;
    this.cipherManager = cipherManager;
  }

  public SecurePutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      Compactor compactor, CipherManager cipherManager, ByteBuffer id, ByteBuffer key,
      ByteBuffer value, long invalidatedLsn) {
    this(objectManager, compactor, cipherManager, id, key,
        cipherManager.generateInitializationVector(), value, invalidatedLsn);
  }

  public ByteBuffer getInitVector() {
    return iv;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    header.putInt(getIdentifier().remaining());
    header.putInt(getKey().remaining());
    header.putInt(getInitVector().remaining());
    ByteBuffer cipheredValue = cipherManager.encrypt(getValue(), iv);
    header.putInt(cipheredValue.remaining());
    long lsn = getInvalidatedLsns().stream().findFirst().orElse(-1L);
    header.putLong(lsn).flip();
    return new ByteBuffer[] { header, getIdentifier().slice(), getKey().slice(), iv.slice(), cipheredValue.slice() };
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    SecurePutAction action = (SecurePutAction) obj;
    return Objects.equals(iv, action.iv);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), iv);
  }
}
