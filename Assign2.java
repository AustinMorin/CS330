package Assign2;
import java.text.DecimalFormat;
import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Assign2 {
	
	static Connection conn=null;
	
	public static void main(String[] args) throws Exception{
		String paramfile = "connectparams.txt";
		if(args.length >= 1)
			paramfile= args[0];
		
		Properties connectprops = new Properties();
		connectprops.load(new FileInputStream(paramfile));
		
		try{
			Class.forName("com.mysql.jdbc.Driver");
			String dburl = connectprops.getProperty("dburl");
			String username = connectprops.getProperty("user");
			conn = DriverManager.getConnection(dburl, connectprops);
			System.out.printf("Database connection %s %s established.%n", dburl, username);
			
			
			Scanner scanner = new Scanner(System.in);
			while(true){
				System.out.println("Enter ticker symbol [start/end dates]: ");
				String[] data = scanner.nextLine().trim().split("\\s+");
				if(data.length > 3)
					break;
				else if(data[0].equals("")){
					conn.close();
					scanner.close();
					break;
				}
				
				processTicker(data[0]);
				if(data.length <3)
					processStockSplits(data[0], "", "");
				else 
					processStockSplits(data[0], data[1], data[2]);
			}
			
			conn.close();
			scanner.close();
		} catch (SQLException e) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", e.getMessage(), e.getSQLState(), e.getErrorCode());
			conn.close();
			}
		
		}
	
		
	
	static int processTicker(String ticker) throws SQLException{
		PreparedStatement pstmt = conn.prepareStatement(
				"SELECT Name" +
						" FROM Company" +
						" WHERE Ticker = ?");
		
		pstmt.setString(1, ticker);
		ResultSet rs = pstmt.executeQuery();
		
		if(rs.next()){
			System.out.printf("%s%n", rs.getString("Name"));
			pstmt.close();
			return 1;
		} else {
			System.out.printf("Ticker %s not found.%n", ticker);
			pstmt.close();
			return 0;
		}
		
		
	}
	
	public static class DateData{
		double openPrice;
		double closePrice;
		String date;
	}
	
	
	static void processStockSplits(String ticker, String startDate, String endDate) throws SQLException{
		ResultSet rs;
		if(startDate.equals("")|| endDate.equals("")){
			PreparedStatement pstmt = conn.prepareStatement(
				"SELECT *" +
						" FROM PriceVolume" +
						" WHERE Ticker = ?" +
						" ORDER BY TransDate DESC");
		
			pstmt.setString(1,  ticker);
			rs = pstmt.executeQuery();
		} else{
			PreparedStatement pstmt = conn.prepareStatement(
					"SELECT *" +
							" FROM PriceVolume" +
							" WHERE Ticker = ?" +
							" AND TransDate >= ? AND TransDate <= ?"+
							" ORDER BY TransDate DESC");
			pstmt.setString(1, ticker);
			pstmt.setString(2, startDate);
			pstmt.setString(3, endDate);
			rs = pstmt.executeQuery();
		}
		
		
		ArrayList<DateData> dataList = new ArrayList<DateData>();
		String todayDate = "";
		double todayClose= 0;
		double tomoOpen= 0;
		double divisor = 1;
		int countSplits = 0;
		int countDays = 0;

		while(rs.next()){
			todayDate = rs.getString("TransDate");
			countDays++;
			double tempOpen = Double.parseDouble(rs.getString("OpenPrice"));
			double tempClose = Double.parseDouble(rs.getString("ClosePrice"));
			
			
			todayClose = tempClose;
			double tempSplitData = processData(todayDate, tomoOpen, todayClose);
			if(tempSplitData > 0 ){
				countSplits++;
				divisor = divisor * tempSplitData;
			}
			DateData tempDate = new DateData();
			tempDate.openPrice = tempOpen/divisor;
			tempDate.closePrice = tempClose/divisor;
			tempDate.date = todayDate;
			dataList.add(0, tempDate);
			tomoOpen = tempOpen;
			
				
		}
		System.out.println(countSplits+" splits in "+countDays+" days\n");
		
		if(countDays>=51){
			executeInvestmentStrategy(dataList);
		}
	}
	
	private static double processData(String date, double open, double close){
		double ratio = close/open;
		String pattern = "###.00";
		DecimalFormat df = new DecimalFormat(pattern);
		
		
		if(Math.abs(ratio - 2.0)<0.20){
			System.out.println("2:1 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 2.0;
		}
		else if(Math.abs(ratio - 3.0)<0.30){
			System.out.println("3:1 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 3.0;
		}
		else if(Math.abs(ratio - 1.5)<0.15){
			System.out.println("3:2 split on "+date+" "+df.format(close)+" --> "+df.format(open));
			return 1.5;
		}
		return 0;
	}
	
	
	private static double computeAvgClose(int endIndex, ArrayList<DateData> dataList){
		int startIndex = endIndex - 50;
		double sumClose =0;
		for(int i =startIndex; i<endIndex; i++){
			sumClose += dataList.get(i).closePrice;
		}
		return (double)Math.round(sumClose/50 * 100) /100;
	}
	
	private static void executeInvestmentStrategy(ArrayList<DateData> dataList){
		System.out.println("Executing investment strategy");
		double curCash = 0;
		int curShares = 0;
		double avgClose = 0;
		int transNum = 0;
		
		for(int i =50; i<dataList.size()-1; i++){
			avgClose = computeAvgClose(i, dataList);
			DateData tempDate = dataList.get(i);
			if(tempDate.closePrice < avgClose && tempDate.closePrice/tempDate.openPrice< 0.97000001){

				curShares +=100; 
				curCash -= dataList.get(i+1).openPrice*100 +8;
				transNum++;
			} else {
				if(curShares >=100 && tempDate.openPrice>avgClose){
					if((tempDate.openPrice / dataList.get(i-1).closePrice) >1.00999999){
						curShares -=100;
						curCash += ((tempDate.openPrice + tempDate.closePrice)/2)*100;
						curCash -= 8;
						transNum++;
					}
				}
			}
			
			
		}
		curCash += dataList.get(dataList.size()-1).openPrice * curShares;
		String pattern = "###.00";
		DecimalFormat df = new DecimalFormat(pattern);
		System.out.println("Transactions executed: "+ transNum+"\nNet Cash: "+df.format(curCash));
	}
		
	
}

