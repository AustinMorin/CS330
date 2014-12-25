package Assign3;
import java.text.DecimalFormat;
import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;





public class Assign3 {
	
	static Connection readerconn = null;
	static Connection writerconn = null;
	
	public static class Basket{
		String industryName;
		ArrayList<String> tickers;
	}
	
	public static class DateData{
		double openPrice;
		double closePrice;
		String date;
	}
	
	public static class TradingInterval{
		String firstDay;
		String lastDay;
	}
	
	public static class MinTransDate{
		int year; 
		int month;
		int day;
	}
	
	public static class MaxTransDate{
		int year;
		int month;
		int day;
	}
	
	public static void dropTable() throws SQLException{

		Statement stmt = writerconn.createStatement();
		stmt.executeUpdate(
				"drop table if exists Performance");
	}
	
	public static void createTable() throws SQLException{
		Statement stmt = writerconn.createStatement();
		stmt.executeUpdate(
				"create table Performance(Industry char(30),"
				+ " Ticker char(6),"
				+ " StartDate char(10),"
				+ " EndDate char(10),"
				+ " TickerReturn char(12),"
				+ " IndustryReturn char(12))");
		
	}
	
	
	public static void updateTable(Basket basket) throws SQLException{
		
		String industry = basket.industryName;
		basket.tickers = new ArrayList<String>();
		//checked--->good sql syntax
		PreparedStatement pstmt = readerconn.prepareStatement(
				"select Ticker, min(TransDate) as MinTransDate, max(TransDate) as MaxTransDate,"+
						" count(distinct TransDate) as TradingDays"+
							" from Company natural left outer join PriceVolume"+
							" where Industry = ?"+
							" group by Ticker"+
							" having TradingDays >= 150"+
							" order by Ticker");
		pstmt.setString(1,  industry);
		ResultSet rs = pstmt.executeQuery();
		ArrayList<MinTransDate> minTransDates = new ArrayList<MinTransDate>();
		ArrayList<MaxTransDate> maxTransDates = new ArrayList<MaxTransDate>();
		while(rs.next()){
			String tempMinTransDate = rs.getString("MinTransDate");
			String tempMinYear = tempMinTransDate.substring(0, 4);
			String tempMinMonth = tempMinTransDate.substring(5, 7);
			String tempMinDay = tempMinTransDate.substring(8);
			String tempTicker = rs.getString("Ticker");
		    basket.tickers.add(tempTicker);
			MinTransDate tempMin = new MinTransDate();
			tempMin.year =Integer.parseInt(tempMinYear);
			tempMin.month = Integer.parseInt(tempMinMonth);
			tempMin.day = Integer.parseInt(tempMinDay);
			minTransDates.add(tempMin);
			
			String tempMaxTransDate = rs.getString("MaxTransDate");
			String tempMaxYear = tempMaxTransDate.substring(0, 4);
			String tempMaxMonth = tempMaxTransDate.substring(5, 7);
			String tempMaxDay = tempMaxTransDate.substring(8);
			MaxTransDate tempMax = new MaxTransDate();
			tempMax.year = Integer.parseInt(tempMaxYear);
			tempMax.month = Integer.parseInt(tempMaxMonth);
			tempMax.day = Integer.parseInt(tempMaxDay);
			maxTransDates.add(tempMax);		
		}
		
		String maxMinTransDate = getMaxMinTransDate(minTransDates);
		String minMaxTransDate = getMinMaxTransDate(maxTransDates);
		
		ArrayList<TradingInterval> intervals = getTradingIntervals(basket, maxMinTransDate, minMaxTransDate);
		System.out.println("interval for "+industry+" = "+intervals.get(0).firstDay+"-"+intervals.get(0).lastDay);
		for(int i =0; i<intervals.size(); i++){
			produceReturns(basket, intervals.get(i));
		}
		
	}
	
	public static ArrayList<TradingInterval>  getTradingIntervals(Basket basket, String maxMinTransDate, String minMaxTransDate) throws SQLException{
		PreparedStatement pstmt = readerconn.prepareStatement(
				//checked--->good sql syntax!
				"select P.TransDate"+
						" from PriceVolume P"+
						" where Ticker = ? and TransDate >= ?"+
					  " and TransDate<= ?");
		pstmt.setString(1, basket.tickers.get(0));
		pstmt.setString(2, maxMinTransDate);
		pstmt.setString(3,  minMaxTransDate);
		ResultSet rs = pstmt.executeQuery();
		
		int i = 0;
		String tempFirst = "";
		String tempLast = "";
		ArrayList<TradingInterval> intervals = new ArrayList<TradingInterval>();
		while(rs.next()){
			if(i==0)
				tempFirst = rs.getString("TransDate");
			if(i==59){
				tempLast = rs.getString("TransDate");
				i=-1;
				TradingInterval temp = new TradingInterval();
				temp.firstDay = tempFirst;
				temp.lastDay = tempLast;
				intervals.add(temp);
			}
			i++;
		}
		return intervals;
	}
	
	private static double processData(String date, double open, double close){
		double ratio = close/open;
	
		if(Math.abs(ratio - 2.0)<0.20){
			//System.out.println("2:1 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 2.0;
		}
		else if(Math.abs(ratio - 3.0)<0.30){
			//System.out.println("3:1 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 3.0;
		}
		else if(Math.abs(ratio - 1.5)<0.15){
			//System.out.println("3:2 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 1.5;
		}
		return 0;
	}
	
	public static void produceReturns(Basket basket, TradingInterval interval) throws SQLException{
		produceTickerReturn(basket, interval);
		produceIndustryReturn(basket, interval);
		
	}
	
	public static void produceTickerReturn(Basket basket, TradingInterval interval)throws SQLException{
		for(int i=0; i<basket.tickers.size(); i++){
			String ticker= basket.tickers.get(i);
			PreparedStatement pstmt = readerconn.prepareStatement(
					//checked---> good sql syntax!
					"select P.TransDate, P.openPrice, P.closePrice"+
							" from PriceVolume P"+
							" where Ticker = ? and TransDate>= ? and TransDate <= ?"+
							"order by TransDate DESC");
			pstmt.setString(1, ticker);
			pstmt.setString(2, interval.firstDay);
			pstmt.setString(3, interval.lastDay);
			ResultSet rs = pstmt.executeQuery();
			
			ArrayList<DateData> dataList = new ArrayList<DateData>();
			String todayDate = "";
			double todayClose= 0;
			double tomoOpen= 0;
			double divisor = 1;
			while(rs.next()){
				todayDate = rs.getString("TransDate");
				double tempOpen = Double.parseDouble(rs.getString("OpenPrice"));
				double tempClose = Double.parseDouble(rs.getString("ClosePrice"));
				
				
				todayClose = tempClose;
				double tempSplitData = processData(todayDate, tomoOpen, todayClose);
				if(tempSplitData > 0 ){
					divisor = divisor * tempSplitData;
				}
				DateData tempDate = new DateData();
				tempDate.openPrice = tempOpen/divisor;
				tempDate.closePrice = tempClose/divisor;
				tempDate.date = todayDate;
				dataList.add(0, tempDate);
				tomoOpen = tempOpen;
				
					
			}
			//dataList now contains the adjusted pricevolume data for all transdates in the given interval
			if(dataList.size()>0){
				double openPrice = dataList.get(0).openPrice;
				double closePrice = dataList.get(dataList.size()-1).closePrice;
				double tickerReturnNumber = (closePrice/openPrice)-1;
				DecimalFormat df = new DecimalFormat("#.####");
			
				String tickerReturn = df.format(tickerReturnNumber);
				PreparedStatement pstmt2 = writerconn.prepareStatement(
						"insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn)"
								+"values(?, ?, ?, ?, ?)");
				pstmt2.setString(1, basket.industryName);
				pstmt2.setString(2, ticker);
				pstmt2.setString(3, interval.firstDay);
				pstmt2.setString(4, interval.lastDay);
				pstmt2.setString(5, tickerReturn);
			
				pstmt2.executeUpdate();
			}
		}
	}
	
	public static void produceIndustryReturn(Basket basket, TradingInterval interval)throws SQLException{
		for(int i =0; i<basket.tickers.size(); i++){
			String ticker = basket.tickers.get(i);
			PreparedStatement pstmt = writerconn.prepareStatement(
					"select sum(TickerReturn) as Sum"
					+	" from Performance"
					+	" where Ticker != ?");
			pstmt.setString(1, ticker);
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()){
				double industryReturnNumber = Double.parseDouble( rs.getString("Sum"));
				DecimalFormat df = new DecimalFormat("#.####");
				
				String industryReturn = df.format(industryReturnNumber);
				PreparedStatement pstmt2 = writerconn.prepareStatement(
						"insert into Performance(IndustryReturn)"
						+	"values(?)");
				pstmt2.setString(1, industryReturn);
				pstmt2.executeUpdate();
			}
			
		}
	}
	
	public static String getMaxMinTransDate(ArrayList<MinTransDate> minTransDates){
		int maxMinYear = 0;
		int maxMinMonth = 0;
		int maxMinDay = 0;
		//find max(minyear)
		for(int i =0; i<minTransDates.size(); i++){
			MinTransDate temp = minTransDates.get(i);
			if(maxMinYear < temp.year){
				maxMinYear = temp.year;
			}
		}
		//of the dates that have the year = max(minyear), find max(minMonth)
		for(int i=0; i<minTransDates.size(); i++){
			MinTransDate temp = minTransDates.get(i);
			if(temp.year == maxMinYear){
				if(maxMinMonth < temp.month)
					maxMinMonth = temp.month;
			}
		}
		//of the dates that have the year= max(minyear) and month = max(minMonth), find max(day)
		for(int i =0 ; i<minTransDates.size(); i++){
			MinTransDate temp = minTransDates.get(i);
			if(temp.year == maxMinYear && temp.month == maxMinMonth){
				if(maxMinDay< temp.day){
					maxMinDay = temp.day;
				}
			}
		}
		StringBuilder maxMinTransDate = new StringBuilder();
		maxMinTransDate.append(maxMinYear);
		maxMinTransDate.append(".");
		maxMinTransDate.append(maxMinMonth);
		maxMinTransDate.append(".");
		maxMinTransDate.append(maxMinDay);
		
		return maxMinTransDate.toString();
	}
		
		public static String getMinMaxTransDate(ArrayList<MaxTransDate> maxTransDates){
			int minMaxYear = 0;
			int minMaxMonth = 0;
			int minMaxDay = 0;
			
			for(int i =0; i<maxTransDates.size(); i++){
				MaxTransDate temp = maxTransDates.get(i);
				if(minMaxYear == 0 || minMaxYear > temp.year){
					minMaxYear = temp.year;
				}
			}
			//of the dates that have the year = max(minyear), find max(minMonth)
			for(int i=0; i<maxTransDates.size(); i++){
				MaxTransDate temp = maxTransDates.get(i);
				if(temp.year == minMaxYear){
					if(minMaxMonth == 0 || minMaxMonth < temp.month)
						minMaxMonth = temp.month;
				}
			}
			//of the dates that have the year= max(minyear) and month = max(minMonth), find max(day)
			for(int i =0 ; i<maxTransDates.size(); i++){
				MaxTransDate temp = maxTransDates.get(i);
				if(temp.year == minMaxYear && temp.month == minMaxMonth){
					if(minMaxDay == 0 || minMaxDay< temp.day)
						minMaxDay = temp.day;
				}
			}
		
		//make a string from these 3 values and return the string as the max(mintransdate)
		StringBuilder minMaxTransDate = new StringBuilder();
		minMaxTransDate.append(minMaxYear);
		minMaxTransDate.append(".");
		minMaxTransDate.append(minMaxMonth);
		minMaxTransDate.append(".");
		minMaxTransDate.append(minMaxDay);
		
		return minMaxTransDate.toString();
	}
	
	public static void main (String[] args) throws Exception{
		String readerparamfiles = "readerparams.txt";
		if(args.length>= 1){
			readerparamfiles = args[0];
		}
		
		
		Properties connectprops = new Properties();
		connectprops.load(new FileInputStream (readerparamfiles));
		
		String writerparamfiles = "writerparams.txt";
		
		Properties connprops = new Properties();
		connprops.load(new FileInputStream (writerparamfiles));
		
		try{
			
			Class.forName("com.mysql.jdbc.Driver");
			String readerdburl = connectprops.getProperty("dburl");
			String readeruser = connectprops.getProperty("user");
			readerconn = DriverManager.getConnection(readerdburl, connectprops);
			
			System.out.printf("Database connection %s %s established.%n", readerdburl, readeruser);
			
			
			String writerdburl = connprops.getProperty("dburl");
			String writeruser = connprops.getProperty("user");
			writerconn = DriverManager.getConnection(writerdburl, connprops);

			System.out.printf("Database connection %s %s established.%n", writerdburl, writeruser);
			

			dropTable();
			createTable();
			
			ArrayList<Basket> baskets = new ArrayList<Basket>();
		
			
			Statement stmt = readerconn.createStatement();
			ResultSet rs = stmt.executeQuery(
					"select distinct Industry"+
							" from Company"+
							" order by Industry");
			
			
			while(rs.next()){
				Basket tempBasket = new Basket();
				tempBasket.industryName = rs.getString("Industry");
				baskets.add(tempBasket);
			}
			for(int i =0; i<baskets.size(); i++){
				updateTable(baskets.get(i));
			}
			
			
			readerconn.close();
			writerconn.close();
			
			}catch (SQLException e){
				System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", e.getMessage(), e.getSQLState(), e.getErrorCode());
				readerconn.close();
				writerconn.close();
		}
	}
}
