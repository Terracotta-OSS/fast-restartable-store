/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.ActionManagerImpl;
import com.terracottatech.frs.compaction.CompactionActions;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.flash.ReadManager;
import com.terracottatech.frs.flash.ReadManagerImpl;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.HiLoBufferSource;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SLABBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.io.nio.NIOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.MasterLogRecordFactory;
import com.terracottatech.frs.log.StagingLogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.transaction.TransactionManagerImpl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * @author tim
 */
public abstract class RestartStoreFactory {

  private RestartStoreFactory() {
  }

  private static ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> createCodec(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) {
    ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec =
            new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(objectManager);
    MapActions.registerActions(0, codec);
    TransactionActions.registerActions(1, codec);
    CompactionActions.registerActions(2, codec);
    return codec;
  }

  public static RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(
          ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
          File dbHome, Properties properties) throws IOException, RestartStoreException {
    Configuration configuration = Configuration.getConfiguration(dbHome, properties);
    
    int memorySize = configuration.getLong(FrsProperty.IO_NIO_POOL_MEMORY_SIZE).intValue();
    BufferSource writingSource = null;
    if ( configuration.getString(FrsProperty.IO_NIO_BUFFER_SOURCE).equals("HILO") ) {
      writingSource = new MaskingBufferSource(new HiLoBufferSource(2048, 8 * 1024 * 1024, memorySize));
    } else if ( configuration.getString(FrsProperty.IO_NIO_BUFFER_SOURCE).equals("SLAB") ) {
      writingSource = new MaskingBufferSource(new SLABBufferSource(8 * 1024 * 1024, memorySize));
    } else {
      long timeout = configuration.getLong(FrsProperty.IO_NIO_MEMORY_TIMEOUT);
      writingSource = new MaskingBufferSource(new SplittingBufferSource(64, memorySize, timeout));
    }
    
    IOManager ioManager = new NIOManager(configuration,writingSource);
    ReadManager readManager = new ReadManagerImpl(ioManager, configuration.getString(FrsProperty.FORCE_LOG_REGION_FORMAT));
    LogManager logManager = new StagingLogManager(ioManager,writingSource,configuration);
    ActionManager actionManager = new ActionManagerImpl(logManager, objectManager,
                                                        createCodec(objectManager),
                                                        new MasterLogRecordFactory());
    TransactionManager transactionManager = new TransactionManagerImpl(actionManager);
    return new RestartStoreImpl(objectManager, transactionManager, logManager,
                                actionManager, readManager, ioManager, configuration);
  }

  public static RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(
          ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, File dbHome,
          long fileSize) throws
          IOException, RestartStoreException {
    Properties properties = new Properties();
    properties.setProperty("io.nio.segmentSize", Long.toString(fileSize));
    return createStore(objectManager, dbHome, properties);
  }
}
