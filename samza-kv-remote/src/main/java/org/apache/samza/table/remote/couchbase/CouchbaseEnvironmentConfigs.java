package org.apache.samza.table.remote.couchbase;

public class CouchbaseEnvironmentConfigs {
  public CouchbaseEnvironmentConfigs() {
  }

  protected Boolean sslEnabled = false;
  protected Boolean certAuthEnabled = false;
  protected String sslKeystoreFile = null;
  protected String sslKeystorePassword = null;
  protected String sslTruststoreFile = null;
  protected String sslTruststorePassword = null;
  protected Integer bootstrapCarrierDirectPort = null;
  protected Integer bootstrapCarrierSslPort = null;
  protected Integer bootstrapHttpDirectPort = null;
  protected Integer bootstrapHttpSslPort = null;
  protected String username = null;
  protected String password = null;

  public Boolean getSslEnabled() {
    return sslEnabled;
  }

  public void setSslEnabled(Boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
  }

  public Boolean getCertAuthEnabled() {
    return certAuthEnabled;
  }

  public void setCertAuthEnabled(Boolean certAuthEnabled) {
    this.certAuthEnabled = certAuthEnabled;
  }

  public String getSslKeystoreFile() {
    return sslKeystoreFile;
  }

  public void setSslKeystoreFile(String sslKeystoreFile) {
    this.sslKeystoreFile = sslKeystoreFile;
  }

  public String getSslKeystorePassword() {
    return sslKeystorePassword;
  }

  public void setSslKeystorePassword(String sslKeystorePassword) {
    this.sslKeystorePassword = sslKeystorePassword;
  }

  public String getSslTruststoreFile() {
    return sslTruststoreFile;
  }

  public void setSslTruststoreFile(String sslTruststoreFile) {
    this.sslTruststoreFile = sslTruststoreFile;
  }

  public String getSslTruststorePassword() {
    return sslTruststorePassword;
  }

  public void setSslTruststorePassword(String sslTruststorePassword) {
    this.sslTruststorePassword = sslTruststorePassword;
  }

  public Integer getBootstrapCarrierDirectPort() {
    return bootstrapCarrierDirectPort;
  }

  public void setBootstrapCarrierDirectPort(Integer bootstrapCarrierDirectPort) {
    this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
  }

  public Integer getBootstrapCarrierSslPort() {
    return bootstrapCarrierSslPort;
  }

  public void setBootstrapCarrierSslPort(Integer bootstrapCarrierSslPort) {
    this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
  }

  public Integer getBootstrapHttpDirectPort() {
    return bootstrapHttpDirectPort;
  }

  public void setBootstrapHttpDirectPort(Integer bootstrapHttpDirectPort) {
    this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
  }

  public Integer getBootstrapHttpSslPort() {
    return bootstrapHttpSslPort;
  }

  public void setBootstrapHttpSslPort(Integer bootstrapHttpSslPort) {
    this.bootstrapHttpSslPort = bootstrapHttpSslPort;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
