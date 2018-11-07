package com.report.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.report.utilities.ConMan;

public class JdbcDao {
	static JdbcDao instance;

	ConMan cm= new ConMan();
	
	public static JdbcDao getInstance(){
		if(instance==null){
			instance= new JdbcDao();
		}
		return instance;
	}
	
	public List<String> getListMoon(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT `moon` FROM `fact_cutomer_advertiser_2018` ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("moon"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	public List<String> getListAdvertiser(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT advertiser FROM fact_cutomer_advertiser_2018 ORDER BY advertiser ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("advertiser"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	public List<String> getListTheaterName(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT outlet_name FROM ticket_sale_report_tbl ORDER BY outlet_name ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("outlet_name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String> getListStudio(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT st FROM ticket_sale_report_tbl ORDER BY st ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("st"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListAdvertiserSummary(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT advertiser FROM fact_cutomer_advertiser_2018 ORDER BY advertiser ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("advertiser"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListAdvertiser1(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT advertiser FROM fact_cutomer_advertiser_2017 ORDER BY advertiser ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("advertiser"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	public List<String> getListAdvertiser2(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT advertiser FROM fact_cutomer_advertiser_2017 ORDER BY advertiser ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("advertiser"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListMoon1(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT moon FROM fact_cutomer_advertiser_2017 ORDER BY moon ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("moon"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListMoon2(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT moon FROM fact_cutomer_advertiser_2017 ORDER BY moon ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("moon"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	public List<String> getListYear(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT year FROM fact_cutomer_advertiser_2017 ORDER BY year ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("year"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String> getListYear1(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT year FROM fact_cutomer_advertiser_2017 ORDER BY year ASC ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("year"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	public List<String> getListCity(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT cityName From ca_city";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("cityName"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	public List<String> getListCity1(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT cityName From ca_city";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("cityName"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	public List<String> getListProduct(String nama_advertiser){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT product FROM fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+nama_advertiser+"%' ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("product"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	public List<String> getListProductSummary(String nama_advertiser){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT product FROM fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+nama_advertiser+"%' ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("product"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListProduct1(String nama_advertiser){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT product FROM fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+nama_advertiser+"%' ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("product"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	public List<String> getListProduct2(String nama_advertiser){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT product FROM fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+nama_advertiser+"%' ";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("product"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiser(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,sum(customer_count) From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserByArea2(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,sum(customer_count) From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserByArea(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,sum(customer_count) From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserCutomer(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,advertiser,product,moon,SUM(customer_count) FROM fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,advertiser,product,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[5];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	public List<String[]> getListCustomerAdvertiserSumByCity2(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity3(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity2cutomer(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity2Cutomizer(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCityarea2(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserByBioskop(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count,(SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	public List<String[]> getListCustomerAdvertiserByBioskopCutomer(String advertiser,String product,String moons){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count,(SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' AND moon LIKE '%"+moons+"%' ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				//data[7]=rs.getString(9);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	
	
	
	
	public List<String> getListCity(String advertiser,String product){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT city From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY city ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("city"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	public List<String> getListCity2(String advertiser,String product){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT city From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY city ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("city"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String> getListCity1(String advertiser,String product){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT city From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY city ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("city"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String> getListCityBycutomer(String advertiser,String product){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT city From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY city ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("city"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	
	public List<String> getListClass(String city,String theater){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,SUM(customer_count) FROM fact_cutomer_advertiser GROUP BY clas,city";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("class"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
			}
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByClass1(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,moon,SUM(customer_count) FROM fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY clas,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[5];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	}

	
	
	
	
	
	public List<String[]> getListClassSumByCity(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			//String sql="SELECT city,moon,SUM(customer_count) FROM fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			
			String sql="SELECT class,advertiser,product,moon,customer_count,(SELECT u.`class` FROM `dim_outlet_clas) From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY city ASC";
			
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	}
	
	public List<String[]> getListCustomerAdvertiserSumClass(String advertiser,String product, String ClassT){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
		
			String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' AND class LIKE '%"+ClassT+"%' GROUP BY class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				//data[6]=rs.getString(7);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
		} 
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity2Summary(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			
			
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				//data[3]=rs.getString(4);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	public List<String[]> getListCustomerAdvertiserByBioskopSummary(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			
			//String sql="SELECT theater,class,city,advertiser,product,moon,customer_count, (SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY class ASC";
			
			
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count, (SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser_2018 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	

	public List<String> getListClass(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT class From fact_cutomer_advertiser ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("class"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
			} 	
	
	
	
	
	
	
	
	
	
	
	
	

	
	public List<String> getListMonth(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT month From ticket_sale_report_screen ORDER BY month ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("month"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
			} 	
	
	
	
	
	
	
	
	public List<String> getListYears(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT year From ticket_sale_report_screen ORDER BY year ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("year"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
			} 	
	
	
	
	
	
	
	public List<String> getListTipe(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT tipe_bioskop From ticket_sale_report_screen ORDER BY tipe_bioskop ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("tipe_bioskop"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
			} 	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String> getListClassSummary(){
		List<String> result= new ArrayList<String>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT DISTINCT class From fact_cutomer_advertiser_2018 ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				result.add(rs.getString("class"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
			} 	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiserbyarea(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity2byarea(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,sum(customer_count) FROM fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	
	public List<String[]> getListCustomerAdvertiserByBioskopbyarea(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count,(SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	public List<String[]> getListCustomerAdvertiserBycutomer(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			
			//String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' AND class LIKE '%"+Class+"%' GROUP BY class,city,advertiser,product,moon ORDER BY class ASC";
			
			
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count,(SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%'  ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	
	public List<String[]> getListCustomerAdvertiserByBioskopBycutomer(String advertiser,String product,String moon){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
		
			
			//String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' AND class LIKE '%"+Class+"%' GROUP BY class,city,advertiser,product,moon ORDER BY class ASC";
			
			
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count, (SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' AND moons LIKE '%"+moon+"%' ORDER BY class,moon ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				//data[8]=rs.getString(9);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 

	
	
	
	
	
	
	
	
	
	public List<String[]> getListCustomerAdvertiser2(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT class,city,advertiser,product,moon,customer_count From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' group by class,city,advertiser,product,moon ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[6];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	

	
	
	
	
	public List<String[]> getListCustomerAdvertiserSumByCity2New(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		
		try {
			st=conn.createStatement();
			String sql="SELECT city,moon,customer_count FROM fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' GROUP BY city,moon";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[3];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 





	public List<String[]> getListCustomerAdvertiserByBioskop2(String advertiser,String product){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT theater,class,city,advertiser,product,moon,customer_count,(SELECT u.`theaterName` FROM `dim_theater` u WHERE u.`theaterCode`=theater) From fact_cutomer_advertiser_2017 WHERE advertiser LIKE '%"+advertiser+"%' AND product LIKE '%"+product+"%' ORDER BY class ASC";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				String[] data= new String[8];
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 

	
	
	public List<String[]> getListScreen(String theaterName){
		List<String[]> result= new ArrayList<String[]>();
		Connection conn =null;
		Statement st =null;
		conn= cm.logOn();
		
		try {
			st=conn.createStatement();
			String sql="SELECT tanggal, outlet_code, theater_name, st, film, show_1, show_2, show_3, show_4, show_5, show_6, ptn " 
					+ " From ticket_sale_report_tbl "
					+ " WHERE theater_name LIKE '%"+theaterName+"%' ";
																				
			System.out.println("SQL "+sql);
			
			ResultSet rs = st.executeQuery(sql);
			
			
			while(rs.next()){
				String[] data= new String[12];      
				data[0]=rs.getString(1);
				data[1]=rs.getString(2);
				data[2]=rs.getString(3);
				data[3]=rs.getString(4);
				data[4]=rs.getString(5);
				data[5]=rs.getString(6);
				data[6]=rs.getString(7);
				data[7]=rs.getString(8);
				data[8]=rs.getString(9);
				data[9]=rs.getString(10);
				data[10]=rs.getString(11);
				data[11]=rs.getString(12);
				
				result.add(data);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(st!=null){
				try {
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(conn!=null){
				cm.logOff();
			
			}
		}
		
		return result;
	} 
	




public List<String> getListMovie(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT film From ticket_sale_report_tbl ORDER BY film ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("film"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
} 






public List<String> getListOutlet(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT outlet_code From ticket_sale_report_tbl ORDER BY outlet_code ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("outlet_code"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
} 










public List<String> getListtlet(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT theater From fact_cutomer ORDER BY theater ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("theater"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
} 












public List<String> getListClas2017(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT class From fact_cutomer_summary ORDER BY class ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("class"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
} 






public List<String> getListOut(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT year From fact_cutomer ORDER BY year ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("year"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
		} 	





public List<String> getLisTheaterss(){
	List<String> result= new ArrayList<String>();
	Connection conn =null;
	Statement st =null;
	conn= cm.logOn();
	
	try {
		st=conn.createStatement();
		String sql="SELECT DISTINCT theater From fact_cutomer_summary ORDER BY theater ASC";
		ResultSet rs = st.executeQuery(sql);
		while(rs.next()){
			result.add(rs.getString("theater"));
		}
	} catch (SQLException e) {
		e.printStackTrace();
	}finally{
		if(st!=null){
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(conn!=null){
			cm.logOff();
		
		}
	}
	
	return result;
		} 	
}






	