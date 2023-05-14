package com.octopus.kettlex.core.plugin;

import com.octopus.kettlex.core.exception.KettleXClassLoaderException;
import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * @decription: 提供Jar隔离的加载机制，会把传入的路径、及其子路径、以及路径中的jar文件加入到class path。
 * @copyright: com.alibaba.datax.core.util.container.JarLoader
 */
@Slf4j
public class KettleXURLClassLoader extends URLClassLoader {

  public KettleXURLClassLoader(String[] paths) {
    this(paths, KettleXURLClassLoader.class.getClassLoader());
  }

  public KettleXURLClassLoader(String[] paths, ClassLoader parent) {
    super(getURLs(paths), parent);
  }

  public KettleXURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  private static URL[] getURLs(String[] paths) {
    Validate.isTrue(null != paths && 0 != paths.length, "jar package path can't be blank.");

    List<String> dirs = new ArrayList<String>();
    for (String path : paths) {
      dirs.add(path);
      KettleXURLClassLoader.collectDirs(path, dirs);
    }

    List<URL> urls = new ArrayList<URL>();
    for (String path : dirs) {
      urls.addAll(doGetURLs(path));
    }

    return urls.toArray(new URL[0]);
  }

  private static void collectDirs(String path, List<String> collector) {
    if (null == path || StringUtils.isBlank(path)) {
      return;
    }

    File current = new File(path);
    if (!current.exists() || !current.isDirectory()) {
      return;
    }

    for (File child : current.listFiles()) {
      if (!child.isDirectory()) {
        continue;
      }

      collector.add(child.getAbsolutePath());
      collectDirs(child.getAbsolutePath(), collector);
    }
  }

  private static List<URL> doGetURLs(final String path) {
    Validate.isTrue(!StringUtils.isBlank(path), "jar package path can't be blank.");

    File jarPath = new File(path);

    Validate.isTrue(
        jarPath.exists() && jarPath.isDirectory(),
        "jar package path must exist and be a directory.");

    /* set filter */
    FileFilter jarFilter =
        new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            return pathname.getName().endsWith(".jar");
          }
        };

    /* iterate all jar */
    File[] allJars = new File(path).listFiles(jarFilter);
    List<URL> jarURLs = new ArrayList<URL>(allJars.length);

    for (int i = 0; i < allJars.length; i++) {
      try {
        jarURLs.add(allJars[i].toURI().toURL());
      } catch (Exception e) {
        throw new KettleXClassLoaderException("load jar error", e);
      }
    }

    return jarURLs;
  }
}
