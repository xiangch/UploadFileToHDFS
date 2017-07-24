package com.wyd.jdbc;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.log4j.helpers.LogLog;
public class JDBCWrapper implements Serializable {
    /**
     *
     */
    private static final long                            serialVersionUID = 6876426193742259377L;
    private static       JDBCWrapper                     instance         = null;
    private static       LinkedBlockingQueue<Connection> connPool         = new LinkedBlockingQueue<>();
    private              int                             connCount        = 0;
    private              int                             connMaxCount     = 5;
    private              int                             connMinCount     = 2;
    private              String                          url              = null;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private JDBCWrapper() {
        PropertiesConfiguration  config = new PropertiesConfiguration();
        config.setEncoding("utf-8");
        try {
            config.load("config.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        url = config.getString("jdbc.url");
        for (int i = 0; i < connMinCount; i++) {
            Connection conn;
            try {
                conn = DriverManager.getConnection(url);
                connPool.put(conn);
                connCount++;
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static JDBCWrapper getInstance() {
        if (instance == null) {
            synchronized (JDBCWrapper.class) {
                if (instance == null) {
                    instance = new JDBCWrapper();
                }
            }
        }
        return instance;
    }

    private Connection getConnection() throws Exception {
        // System.out.println("connPool:" + connPool.size());
        Connection conn = connPool.poll();
        while (conn == null && connCount < connMaxCount) {
            conn = DriverManager.getConnection(url);
            connCount++;
        }
        if (conn == null) {
            throw new Exception("cant't get any connection! connCount(" + connCount + ") ");
        }
        conn.setAutoCommit(true);
        return conn;
    }
    public void executeSQL(String sql,Object[] params) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            for (int z = 0; z < params.length; z++) {
                statement.setObject(z + 1, params[z]);
            }
            statement.execute();
        } catch (Exception e) {
            LogLog.error(e.getMessage());
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    connPool.put(conn);
                } catch (InterruptedException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
    public void executeSQL(String sql) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            statement.execute();
        } catch (Exception e) {
            LogLog.error(e.getMessage());
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    connPool.put(conn);
                } catch (InterruptedException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 批量提交
     */
    public void doBatch(String sql, List<Object[]> paramsList) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            int size = paramsList.size();
            if(size==0)return;
            conn = getConnection();
            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sql);
            int num = 0;
            for (int i = 0; i < size; i += 1000) {
                statement.clearBatch();
                for (int j = 0; j < 1000 && num < size; j++, num++) {
                    Object[] params = paramsList.get(num);
                    for (int z = 0; z < params.length; z++) {
                        statement.setObject(z + 1, params[z]);
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
                conn.commit();// 提交事务
            }
        } catch (Exception e) {
            LogLog.error(e.getMessage());
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    connPool.put(conn);
                } catch (InterruptedException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 执行查询
     * @param sql  sql1
     * @param params  params
     * @param callBack  callBack
     */
    public void doQuery(String sql, Object[] params, ExecuteCallBack callBack) {
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            callBack.call(statement.executeQuery());
        } catch (Exception e) {
            LogLog.error(e.getMessage());
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    connPool.put(conn);
                } catch (InterruptedException e) {
                    LogLog.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        JDBCWrapper jdbcw = JDBCWrapper.getInstance();
        List<String> names = new ArrayList<>();
        List<Object[]> paramsList = new ArrayList<>();
        paramsList.add(new Object[] { "spark" });
        paramsList.add(new Object[] { "scala" });
        jdbcw.doBatch("INSERT INTO tab_user(name) VALUES(?)", paramsList);
        paramsList.clear();
        paramsList.add(new Object[] { "java", "scala" });
        jdbcw.doBatch("UPDATE tab_user set name=? where name=?", paramsList);
        jdbcw.doQuery("select * from tab_user", new Object[] {}, rs -> {
            try {
                while (rs.next()) {
                    names.add(rs.getString(2));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        for (String n : names) {
            System.out.println(n);
        }
        paramsList.add(new Object[] { "java", "scala" });
        jdbcw.doBatch("UPDATE tab_user set name=? where name=?", paramsList);
        jdbcw.executeSQL("delete from tab_user");
    }
}
