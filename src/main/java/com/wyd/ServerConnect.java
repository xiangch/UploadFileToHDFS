package com.wyd;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.wyd.log.bean.ServiceInfo;
import com.wyd.log.util.DESUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/**
 * 服务器连接对象
 *
 * @author zjh
 */
public class ServerConnect {
    private static final Log log = LogFactory.getLog(ServerConnect.class);
    private int        serviceId;
    private String     serviceIP;
    private int        servicePort;
    private String     serviceUser;
    private String     servicePass;
    private String     logDirectory;
    private Connection conn;
    private DESUtil    passwordDes;
    PropertiesConfiguration config = new PropertiesConfiguration();

    private PropertiesConfiguration getConfig() {
        return config;
    }

    public ServerConnect(ServiceInfo serviceInfo) {
        this.serviceId = serviceInfo.getId();
        this.serviceIP = serviceInfo.getServiceIP();
        this.servicePort = serviceInfo.getServicePort();
        this.serviceUser = serviceInfo.getServiceUser();
        this.servicePass = serviceInfo.getServicePass();
        this.logDirectory = serviceInfo.getLogDirectory();
        try {
            config.setEncoding("utf-8");
            config.load("config.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void scpFile(String fileName) {
        Session ssh = null;
        try {
            File localFile = new File(fileName);
            if (localFile.exists()) {
                localFile.delete();
            }
            if (!localFile.createNewFile()) {
                System.out.println("文件创建失败：" + fileName);
                return;
            }
            ssh = conn.openSession();
            ssh.execCommand("cat " + logDirectory + "/" + fileName);
            // 获取输出的内容
            InputStream is = new StreamGobbler(ssh.getStdout());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();
            try (FileOutputStream out = new FileOutputStream(localFile)) {
                while (line != null) {
                    out.write((line+"\n").getBytes());
                    line = br.readLine();
                }
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            br.close();
            is.close();
        } catch (IOException ex) {
            log.error("", ex);
        } catch (NullPointerException e) {
            System.err.println("异常" + logDirectory + "/" + fileName + "\t" + serviceId);
            log.error("", e);
        } finally {
            if (ssh != null) {
                ssh.close();
            }
        }
    }

    /** 获取本地文件的行数 */
    private Session getSession() {
        if (this.conn == null) {
            synchronized (this) {
                if (this.conn == null) {
                    login();
                }
            }
        }
        Session session = null;
        try {
            session = this.conn.openSession();
        } catch (Exception e) {
            log.error("服务器id:" + serviceId, e);
        }
        return session;
    }

    /** 尝试登录游戏服 */
    private boolean login() {
        try {
            if (conn == null) {
                boolean isConn = false;
                if (servicePass == null || servicePass.equals("")) {
                    this.conn = new Connection(serviceIP);
                    conn.connect();
                    String privKey = getConfig().getString("priv_Key");
                    File pemKeyFile = null;
                    if (privKey != null && !privKey.equals("")) {
                        pemKeyFile = new File(privKey);
                    }
                    isConn = conn.authenticateWithPublicKey(serviceUser, pemKeyFile, null);
                } else {
                    this.conn = new Connection(serviceIP, servicePort);
                    this.passwordDes = new DESUtil(getConfig().getString("deskey"));
                    conn.connect();
                    isConn = conn.authenticateWithPassword(serviceUser, this.passwordDes.decryptStr(servicePass));
                }
                if (!isConn) {
                    throw new Exception("用户名或密码错误！" + serviceId);
                }
                return true;
            }
        } catch (Exception e) {
            log.error(e, e);
            conn.close();
        }
        return false;
    }

    /** 登出游戏服 */
    public void logout() {
        if (this.conn != null) {
            this.conn.close();
        }
    }

    public static void main(String[] args) {
        DESUtil passwordDes = new DESUtil("rJAge9frIYqdTj");
        try {
            System.out.println(passwordDes.decryptStr("vsXTPIAqIJmm2P8KjXLlmaCeJ+NQGFi1"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /** 获取采集文件列表 */
    public List<String> getFileList(String fileNamePattern) {
        if (login()) {
            Session ssh = null;
            try {
                ssh = conn.openSession();
                ssh.execCommand("cd " + logDirectory + ";ls " + fileNamePattern + ";");
                // 获取输出的内容
                InputStream is = new StreamGobbler(ssh.getStdout());
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                List<String> fileList = new ArrayList<String>();
                String dataLine = br.readLine();
                while (dataLine != null) {
                    fileList.add(dataLine);
                    dataLine = br.readLine();
                }
                br.close();
                is.close();
                Collections.sort(fileList);
                return fileList;
            } catch (IOException ex) {
                log.error("", ex);
            } finally {
                if (ssh != null) {
                    ssh.close();
                }
            }
        }
        return null;
    }
}
