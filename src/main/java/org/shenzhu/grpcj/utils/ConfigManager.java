package org.shenzhu.grpcj.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class ConfigManager {
  private double version;

  private Map<String, List<String>> servers;

  private Map<String, Map<String, String>> network;

  private Map<String, Integer> diskSize;

  private String diskLocation;

  private Map<String, String> diskNames;

  public ConfigManager(String filePath) throws FileNotFoundException {
    // Get and parse input file
    File inputFile = new File(filePath);
    InputStream inputStream = new FileInputStream(inputFile);

    Yaml yaml = new Yaml();
    Map<String, Object> objectMap = yaml.load(inputStream);

    this.version = (double) objectMap.get("version");
    this.servers = (Map<String, List<String>>) objectMap.get("servers");
    this.network = (Map<String, Map<String, String>>) objectMap.get("network");
    this.diskSize = (Map<String, Integer>) objectMap.get("diskSize");
    this.diskLocation = (String) objectMap.get("diskLocation");
    this.diskNames = (Map<String, String>) objectMap.get("diskNames");
  }

  public double getVersion() {
    return this.version;
  }

  public List<String> getAllMasterServers() {
    return this.servers.get("master_servers");
  }

  public List<String> getAllChunkServers() {
    return this.servers.get("chunk_servers");
  }

  public String getServerHostName(String serverName) {
    return this.network.get(serverName).get("hostname");
  }

  public String getServerPort(String serverName) {
    return String.valueOf(this.network.get(serverName).get("port"));
  }

  public String getServerAddress(String serverName) {
    return this.getServerHostName(serverName) + ":" + this.getServerPort(serverName);
  }

  public String getDatabaseName(String serverName) {
    return this.diskNames.get(serverName);
  }

  public int getFileChunkBlockSize() {
    return this.diskSize.get("block_size_mb");
  }

  public int getRequiredDiskSpaceToMaintain() {
    return this.diskSize.get("min_free_disk_space_mb");
  }

  public String getDiskLocation() {
    return this.diskLocation;
  }
}
