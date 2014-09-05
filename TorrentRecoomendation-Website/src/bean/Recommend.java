package bean;

import java.io.IOException;
import java.sql.SQLException;

public class Recommend {
	private DataBase db;
	
	public Recommend() throws ClassNotFoundException {
		this.db = new DataBase();
	}
	
	public DataBase getDb() {
		return this.db;
	}
	
	public void setDb(DataBase db) {
		this.db = db;
	}
    
    public String recommend(String id) throws SQLException{
    	return this.db.searchByUid(id);
    }
    
    public void importData() throws IOException{
    	this.db.ImportData();
    }
}
