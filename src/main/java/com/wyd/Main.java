package com.wyd;
import com.wyd.jdbc.JDBCWrapper;
import com.wyd.log.bean.ServiceInfo;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
public class Main {
    private PropertiesConfiguration config = null;
    private SimpleDateFormat        sf     = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Main() {
        try {
            config = new PropertiesConfiguration();
            config.setEncoding("utf-8");
            config.load("config.properties");
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long getSleep() {
        return config.getLong("sleep");
    }

    public static void main(String[] args) {
        Main m = new Main();
        try {
            while (true) {
                m.action();
                Thread.sleep(m.getSleep());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void action() {
        initServerList();
        String prefix = config.getString("prefix");
        for (ServiceInfo serviceInfo : serviceInfoList) {
            ServerConnect connect = new ServerConnect(serviceInfo);
            List<String> fileList = connect.getFileList(prefix + ".*");
            if(fileList==null){
                System.out.println(serviceInfo.getId()+","+serviceInfo.getServiceIP()+","+serviceInfo.getLogDirectory());
                continue;
            }
            for (String fileName : fileList) {
                if (!fileNameSet.contains(serviceInfo.getId()+"_"+fileName)) {
                    System.out.println(sf.format(new Date()) + " 开始下载文件："+serviceInfo.getId()+"_"+fileName);
                    connect.scpFile(fileName);
                    System.out.println(sf.format(new Date()) + " 已经下载文件："+serviceInfo.getId()+"_"+fileName);
                    fileNameSet.add(serviceInfo.getId()+"_"+fileName);
                    savePoint();
                    upload(fileName,serviceInfo.getId());
                }
            }
            connect.logout();
        }
    }

    private void upload(String fileName,int serviceId) {
        try {
            String dst = config.getString("target");
            if (dst.lastIndexOf("/") != dst.length() - 1) {
                dst += "/";
            }
            File file = new File(fileName);
            if(!file.exists()){
                System.out.println("文件不存在:"+fileName);
                return;
            }
            System.out.println(sf.format(new Date()) + " 开始上传" + fileName);
            UploadLocalFileHDFS(fileName, dst+serviceId+"/" + fileName);
            System.out.println(sf.format(new Date()) + " 已经上传" + fileName);
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void savePoint() {
        try (FileOutputStream out = new FileOutputStream(new File("point"))) {
            for (String fileName : fileNameSet) {
                out.write((fileName + "\n").getBytes());
            }
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Set<String>       fileNameSet     = new HashSet<>();
    private List<ServiceInfo> serviceInfoList = new ArrayList<>();

    private void init() throws IOException {
        File point = new File("point");
        if (!point.exists()) {
            if (!point.createNewFile()) {
                System.out.println("创建point失败");
                return;
            }
        }
        try (FileReader read = new FileReader(point); BufferedReader br = new BufferedReader(read)) {
            String row;
            while ((row = br.readLine()) != null) {
                fileNameSet.add(row.trim());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private void initServerList(){
        serviceInfoList.clear();
        JDBCWrapper jdbcw = JDBCWrapper.getInstance();
        jdbcw.doQuery("select id,local_ip,service_port,service_user,service_pass,log_directory from tab_service_info", new Object[] {}, rs -> {
            try {
                while (rs.next()) {
                    ServiceInfo info = new ServiceInfo();
                    info.setId(rs.getInt(1));
                    info.setServiceIP(rs.getString(2));
                    info.setServicePort(rs.getInt(3));
                    info.setServiceUser(rs.getString(4));
                    info.setServicePass(rs.getString(5));
                    info.setLogDirectory(rs.getString(6));
                    serviceInfoList.add(info);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
    private static void UploadLocalFileHDFS(String src, String dst) throws IOException {
        Configuration conf = new Configuration();
        // conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        Path pathDst = new Path(dst);
        Path pathSrc = new Path(src);
        fs.copyFromLocalFile(pathSrc, pathDst);
        fs.close();
    }


}
