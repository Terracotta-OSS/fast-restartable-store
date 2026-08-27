package com.terracottatech.frs.cipher;

import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

public class LazyDecryptingPutAction implements GettableAction {

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final CipherManager cipherManager;
  private final ByteBuffer identifier;
  private final long invalidatedLsn;
  private final ByteBuffer[] buffers;
  private final ActionCodec codec;

  private Closeable disposable;
  private volatile PutAction action;

  public LazyDecryptingPutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 CipherManager cipherManager, long invalidatedLsn, ByteBuffer identifier,
                                 ByteBuffer[] buffers, ActionCodec codec) {
    this.objectManager = objectManager;
    this.cipherManager = cipherManager;
    this.invalidatedLsn = invalidatedLsn;
    this.identifier = identifier;
    this.buffers = buffers;
    this.codec = codec;
  }

  @Override
  public long getLsn() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void setDisposable(Closeable c) {
    disposable = c;
  }

  @Override
  public void dispose() {
    try {
      this.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ByteBuffer getIdentifier() {
    return identifier;
  }

  @Override
  public ByteBuffer getKey() {
    if (action == null) {
      action = decrypt();
    }
    return action.getKey();
  }

  @Override
  public ByteBuffer getValue() {
    if (action == null) {
      action = decrypt();
    }
    return action.getValue();
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void record(long lsn) {

  }

  @Override
  public void replay(long lsn) {
    action = decrypt();
    objectManager.replayPut(getIdentifier(), getKey(), getValue(), lsn);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    throw new UnsupportedOperationException("action cannot be serialized");
  }

  @Override
  public void close() throws IOException {
    if (disposable != null) {
      disposable.close();
      disposable = null;
    }
  }

  private PutAction decrypt() {
    int ivLength = ByteBufferUtils.getInt(buffers);
    int tokenLength = ByteBufferUtils.getInt(buffers);
    int payloadLength = ByteBufferUtils.getInt(buffers);
    ByteBuffer initializationVector = ByteBufferUtils.getBytes(ivLength, buffers);
    ByteBuffer tokenBuffer = ByteBufferUtils.getBytes(tokenLength, buffers);
    String tokenUsedForEncryption = StandardCharsets.UTF_8.decode(tokenBuffer).toString();
    ByteBuffer encryptedPayload = ByteBufferUtils.getBytes(payloadLength, buffers);

    ByteBuffer payload = cipherManager.decrypt(encryptedPayload, initializationVector, tokenUsedForEncryption);
    return (PutAction) codec.decode(new ByteBuffer[]{payload});
  }
}
