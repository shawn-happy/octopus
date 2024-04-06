package io.github.shawn.octopus.fluxus.engine.common.file;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public enum FileSystemType {
  HDFS {
    @Override
    public String getFileSystem() {
      return DistributedFileSystem.class.getName();
    }

    @Override
    public String getSchema() {
      return "hdfs";
    }
  },
  LOCAL {
    @Override
    public String getFileSystem() {
      return LocalFileSystem.class.getName();
    }

    @Override
    public String getSchema() {
      return "file";
    }
  },
  ;

  public String getFileSystem() {
    throw new DataWorkflowException("file system not supported");
  }

  public String getSchema() {
    throw new DataWorkflowException("file system not supported");
  }
}
