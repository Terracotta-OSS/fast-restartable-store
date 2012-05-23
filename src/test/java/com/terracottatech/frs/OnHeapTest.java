/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.ActionManagerImpl;
import com.terracottatech.frs.compaction.CompactionActions;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.nio.NIOManager;
import com.terracottatech.frs.log.MasterLogRecordFactory;
import com.terracottatech.frs.log.StagingLogManager;
import com.terracottatech.frs.mock.object.MockObjectManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.transaction.TransactionManagerImpl;
import com.terracottatech.frs.util.TestFolder;

import org.junit.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;

/**
 *
 * @author mscott
 */
public class OnHeapTest {

    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> store;
    StagingLogManager   logMgr;
    ActionManager actionMgr;
    Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> external;
        
    @Rule
    public TestFolder folder = new TestFolder();

    public OnHeapTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        Logger log = org.slf4j.LoggerFactory.getLogger(IOManager.class);
       
        
          }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        external = Collections.synchronizedMap(new HashMap<ByteBuffer, Map<ByteBuffer, ByteBuffer>>());
        ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = new MockObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(external);
        IOManager ioMgr = new NIOManager(folder.getRoot().getAbsolutePath(), ( 1024 * 1024));
        logMgr = new StagingLogManager(ioMgr);
        ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(objectMgr);
        TransactionActions.registerActions(0, codec);
        MapActions.registerActions(1, codec);
        CompactionActions.registerActions(2, codec);
        actionMgr = new ActionManagerImpl(logMgr, objectMgr, codec, new MasterLogRecordFactory());
        TransactionManager transactionMgr = new TransactionManagerImpl(actionMgr);
        Configuration configuration = Configuration.getConfiguration(folder.getRoot());
        store = new RestartStoreImpl(objectMgr, transactionMgr, logMgr, actionMgr, ioMgr, configuration);
    }

    DecimalFormat df = new DecimalFormat("0000000000");
    
    private int addTransaction(int count, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> store) throws Exception {
        String[] r = {"foo","bar","baz","boo","tim","sar","myr","chr"};
        Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction = store.beginTransaction(true);
        ByteBuffer i = (ByteBuffer)ByteBuffer.allocate(4).putInt(1).flip();
        String sk = df.format(count);
        String sv = r[(int)(Math.random()*r.length)%r.length];
//        System.out.format("insert: %s=%s\n",sk,sv);
        ByteBuffer k = (ByteBuffer)ByteBuffer.allocate(1024).put(sk.getBytes()).position(1024).flip();
        ByteBuffer v = (ByteBuffer)ByteBuffer.allocate(1024).put(sv.getBytes()).position(1024).flip();
        int size = k.remaining() + v.remaining();
        transaction.put(i,k,v);
        transaction.commit();
        return size;
    }

    @Test
    public void testIt() throws Exception {
        store.startup();
        int count = 0;
        long bin = 0;
        long time = System.nanoTime();
        while ( bin < 1 * 1024 * 1024 ) {
            bin += addTransaction(count++,store);  
        }
        System.out.format("%.6f sec.\n",(System.nanoTime() - time)/(1e9));
        store.shutdown();
//        System.out.println(logMgr.debugInfo());
        System.out.format("bytes in: %d\n",bin);
        
        File[] list = folder.getRoot().listFiles();
        long fl = 0;
        for ( File f : list  ){
            System.out.println(f.getName() + " " + f.length());
            fl += f.length();
        }
        System.out.println("file total: " + fl);

        time = System.nanoTime();
        external.clear();
        store.startup();
        System.out.format("%.6f sec.\n",(System.nanoTime() - time)/(1e9));
        long esize = 0;
        for ( Map.Entry e : external.entrySet() ) {
            esize += ((Map)e.getValue()).size();
        }
        System.out.format("recovered pulled: %d pushed: %d size: %d\n",logMgr.getRecoveryExchanger().returned(),logMgr.getRecoveryExchanger().count(),
                    esize);

//        for ( Map.Entry<ByteBuffer, Map<ByteBuffer,ByteBuffer>> ids : external.entrySet() ) {
//            int id = ids.getKey().getInt(0);
//            Map<ByteBuffer, ByteBuffer> map = ids.getValue();
//            for ( Map.Entry<ByteBuffer,ByteBuffer> entry : map.entrySet() ) {
//                byte[] g = new byte[entry.getKey().remaining()];
//                entry.getKey().mark();
//                entry.getKey().get(g);
//                String skey = new String(g);
//                entry.getKey().reset();
//                ByteBuffer val = entry.getValue();
//                g = new byte[val.remaining()];
//                val.get(g);
//                String sval = new String(g);
//                System.out.println(id + " " + skey + " " + sval);
//            }
//        }
//

        System.out.println("=========");

        for (int x=0;x<100;x++) addTransaction(count + x, store);
        store.shutdown();

        external.clear();
        store.startup();

        esize = 0;
        for ( Map.Entry e : external.entrySet() ) {
            esize += ((Map)e.getValue()).size();
        }
        System.out.format("recovered pulled: %d pushed: %d size: %d\n",logMgr.getRecoveryExchanger().returned(),logMgr.getRecoveryExchanger().count(),
                    esize);

//        for ( Map.Entry<ByteBuffer, Map<ByteBuffer,ByteBuffer>> ids : external.entrySet() ) {
//            int id = ids.getKey().getInt(0);
//            Map<ByteBuffer, ByteBuffer> map = ids.getValue();
//            for ( ByteBuffer key : map.keySet() ) {
//                byte[] g = new byte[key.remaining()];
//                key.mark();
//                key.get(g);
//                String skey = new String(g);
//                key.reset();
//                ByteBuffer val = map.get(key);
//                g = new byte[val.remaining()];
//                val.get(g);
//                String sval = new String(g);
//                System.out.println(id + " " + skey + " " + sval);
//            }
//            System.out.format("size: %d\n",map.size());
//        }
        store.shutdown();
//        System.out.println(logMgr.debugInfo());
    }

    @After
    public void tearDown() {
    }
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
