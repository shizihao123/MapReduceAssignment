package bean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataBase {
    private String dbUrl = "jdbc:mysql://localhost:3306/movieRecommendation";
    private String dbUser = "root";
    private String dbPwd = "1234";
    private String dbDatapath = "D:/Program Files/Workspaces/MyEclipse 10/MovieRecommendation/data";
    
    public DataBase() throws ClassNotFoundException{
        Class.forName("com.mysql.jdbc.Driver");
    }

    public Connection getConnection() throws SQLException{
        return java.sql.DriverManager.getConnection(dbUrl, dbUser, dbPwd);
    }
    
    public void closeConnection(Connection con) throws SQLException{
        if(con != null)
            con.close();
    }
    
    public void closePrepStmt(PreparedStatement prepStmt) throws SQLException{
        if(prepStmt != null)
            prepStmt.close();
    }
    
    public void closeResultSet(ResultSet rs) throws SQLException{
        if(rs != null)
            rs.close();
    }
    
    public String searchByUid(String id) throws SQLException{
    	Connection con = getConnection();
        String selectStatement = "SELECT * FROM RECOMMENDATION WHERE USER_ID = ?";
        PreparedStatement prepStmt = con.prepareStatement(selectStatement);
        prepStmt.setString(1, id);
        ResultSet rs = prepStmt.executeQuery();
        String result = new String();
        if(rs.next()){
            result = rs.getString(2);
        }
        closeResultSet(rs);
        closePrepStmt(prepStmt);
        closeConnection(con);
        return result;
    }
    
    public void ImportData() throws IOException{
    	File file = new File(dbDatapath);
    	//System.out.println(file.getAbsolutePath());
    	File[] files = file.listFiles();
    	
    	//System.out.println("files.length:" + files.length);
    	for(int i = 0;i<files.length;i++){
    		//System.out.println(i);
    		String temppath = files[i].getAbsolutePath();
    		readData(temppath);
    	}
    }
    
    public void readData(String path){
    	String user_id,movie_list;
    	try{
    		String code = "UTF-8";
    		File file = new File(path);
			InputStream is = new FileInputStream(file);
			InputStreamReader isr = new InputStreamReader(is, code);
    		BufferedReader br = new BufferedReader(isr);
    		String str = "";
    		while (null != (str = br.readLine())){
    			if(str.length() >= 1000){
    				str = str.substring(0, 1000);
    			}
    			String tempStr[] = str.split("	");
    			//System.out.println(tempStr[0]);
    			//System.out.println(tempStr[1]);
    			user_id = tempStr[0];
    			movie_list = tempStr[1];
    			Connection con = getConnection();
    			String idata = "insert into recommendation values(?,?)";
    			PreparedStatement prepStmt = con.prepareStatement(idata);
    			prepStmt.setString(1, user_id);
    			prepStmt.setString(2 , movie_list);
    			prepStmt.executeUpdate();
                closePrepStmt(prepStmt);
                closeConnection(con);
                //System.out.println("finish one!");
    		}
    		br.close();
    	}catch(Exception e){
    		e.printStackTrace();
    		System.err.println("cao£¡¶ÁÈ¡ÎÄ¼þ:" + path + "Ê§°Ü!");
    	}
    }
}
