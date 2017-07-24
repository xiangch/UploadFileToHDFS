package com.wyd.log.bean;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class ServiceInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    

	
    private int               id;
    private String            serviceName;
    private String            serviceIP;
    private int               servicePort;
    private String            serviceUser;
    private String            servicePass;
    private String            logDirectory;


    
    public ServiceInfo() {
        super();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceIP() {
        return serviceIP;
    }

    public void setServiceIP(String serviceIP) {
        this.serviceIP = serviceIP;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public String getServiceUser() {
        return serviceUser;
    }

    public void setServiceUser(String serviceUser) {
        this.serviceUser = serviceUser;
    }

    public String getServicePass() {
        return servicePass;
    }

    public void setServicePass(String servicePass) {
        this.servicePass = servicePass;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }


}
