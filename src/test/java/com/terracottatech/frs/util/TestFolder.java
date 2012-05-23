package com.terracottatech.frs.util;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TestFolder Rule allows creation of files and folders that are
 * only guaranteed to be deleted if the test method passes.  On failure
 * they are left in place for post-mortem debugging.
 * 
 * <pre>
 * public static class HasTestFolder {
 *  &#064;Rule
 *  public TestFolder folder= new TestFolder();
 * 
 *  &#064;Test
 *  public void testUsingTestFolder() throws IOException {
 *      File createdFile= folder.newFile(&quot;myfile.txt&quot;);
 *      File createdFolder= folder.newFolder(&quot;subfolder&quot;);
 *      // ...
 *  }
 * }
 * </pre>
 */
public class TestFolder extends TestWatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestFolder.class);
  
  private File folder;
  
  @Override
  protected void succeeded(Description description) {
    try {
      delete(folder);
    } catch (IOException e) {
      LOGGER.info("Cleanup after {} failed left behind {}", description, folder);
    }
  }

  @Override
  protected void failed(Throwable e, Description description) {
    LOGGER.info("{} failed - leaving {} in place", description, folder);
  }

  @Override
  protected void starting(Description description) {
    try {
      folder = File.createTempFile(description.getClassName() + "." + description.getMethodName() +"_", null, getParentDirectory());
      Assert.assertTrue(folder.delete());
      Assert.assertTrue(folder.mkdirs());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  protected File getParentDirectory() {
    File target = new File("target");
    if (target.isDirectory()) {
      return target;
    } else {
      return new File(System.getProperty("java.io.tmpdir"));
    }
  }

  /**
   * Returns a new fresh file with the given name under the test folder.
   */
  public File newFile(String fileName) throws IOException {
    File f = new File(getRoot(), fileName);
    f.createNewFile();
    return f;
  }

  /**
   * Returns a new fresh file with a random name under the test folder.
   */
  public File newFile() throws IOException {
    return File.createTempFile("temp-file", null, getRoot());
  }

  /**
   * Returns a new fresh folder with the given name under the test folder.
   */
  public File newFolder(String... folderNames) {
    File f = getRoot();
    for (String folder : folderNames) {
      f = new File(f, folder);
      f.mkdir();
    }
    return f;
  }

  /**
   * Returns a new fresh folder with a random name under the test folder.
   */
  public File newFolder() throws IOException {
    File f = File.createTempFile("temp-folder", null, getRoot());
    f.delete();
    f.mkdir();
    return f;
  }

  /**
   * @return the location of this test folder.
   */
  public File getRoot() {
    return folder;
  }

  private static void delete(File file) throws IOException {
    if (file.isFile()) {
      if (!file.delete()) {
        throw new IOException("Failed to delete file " + file);
      }
    } else if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        delete(f);
      }
      if (!file.delete()) {
        throw new IOException("Failed to delete directory " + file);
      }
    }
  }
}
