import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/* Made by Luca Santarella, code used for the analysis of cryptocurrency exchanges for my Bachelor thesis in Computer Science @ Università di Pisa
 */
public class TransactionData {

/*This class allows the retrieval of every transaction of every pair supported by the exchanges Kraken, HitBTC, LBank, Binance during the time frame selected*/
	
	public static void main(String[] args) {
		String exchange = "HitBTC"; //name of the exchange (can be Kraken, HitBTC, LBank, Binance)
		LinkedList<String> pairs = null; //list of String representing the pairs
		//set dates (month=1 => January) 
		int startDay = 1, endDay = 1; //set time interval desired 
		int year=2020, month=3;
		String path = ".\\data\\"+exchange+"\\"; //must be present a data/exchange path
		URL u;
		
		pairs = retrieveTradableAssets(pairs, exchange);

		for(int i=startDay; i<=endDay; i++) { //for every day in the time frame selected
			long ntrans = 0;
			for(int k=0; k<pairs.size(); k++) { //for every pair supported
				String pair = pairs.get(k); //get the asset pair requested
				System.out.println("PAIR "+(k+1)+"/"+pairs.size());
				long since = 0, endTime = 0;
				
				since = setInitialDate(i,month,year,exchange);
				endTime = setFinalDate(i,month,year,exchange);
			
				boolean firstTime = true;
				boolean noData = false;
				int z = 1; //counter to determine the number of API calls made and extract data
				while(since < endTime && !noData) { //until it reaches endTime and does not encounter the same time stamp
					int totBytesRead = 0;
					try {Thread.sleep(3000);} //waiting (avoiding exceeding API rate limit)
					catch (InterruptedException e1) {e1.printStackTrace();}

					try {
						System.out.println("API call made for "+pair);
						u = selectURL(exchange,pair,since);
						InputStream inputStream = u.openStream(); //open Stream from the URL to receive data
						ReadableByteChannel rbc = Channels.newChannel(u.openStream()); //open Stream from the URL to receive data
						FileOutputStream fosTmp = new FileOutputStream(path+"tmp_"+exchange+".json");
						fosTmp.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE); //writes on the output stream
						fosTmp.close();

						//copying from tmp files to the JSON file for trades
						byte[] arr = new byte[100000];
						boolean stop = false;
						while(!stop) {
							byte[] tmp = new byte[2000];
							int bytesRead = inputStream.read(tmp);
							if(bytesRead != -1) {
								System.arraycopy(tmp, 0, arr, totBytesRead, bytesRead);
								totBytesRead = totBytesRead + bytesRead;
							}
							else
								stop = true;
						}
						FileOutputStream fos = new FileOutputStream(path+""+exchange+"_trades_"+pair+"_"+i+"-"+(month)+"-"+year+".json", true);
						
						//formatting to obtain a valid JSON file (valid syntax)
						
						if(!firstTime) {
							String str = ",";
							byte[] arrtmp = str.getBytes();
							fos.write(arrtmp);
						}
						else {
							String str = "{";
							byte[] arrtmp = str.getBytes();
							fos.write(arrtmp);
							firstTime = false;
						}
						String str = "\"data"+z+"\":"; //data + i-th call to distinguish the API call made
						byte[] arrtmp = str.getBytes();
						fos.write(arrtmp);
						fos.write(arr, 0, totBytesRead);
						fos.close();
					}
					catch(IOException e) {e.printStackTrace();}

					//parse JSON file returned to get time stamp for the new API call
					if(exchange.equals("Kraken"))
						since = parseJSONFileKraken(pair, ntrans, noData);
					else if(exchange.equals("Binance"))
						since = parseJSONFileBinance(pair, ntrans, noData);
					else if(exchange.equals("HitBTC"))
						since = parseJSONFileHitBTC(pair, ntrans, noData);
					else if(exchange.equals("LBank"))
						since = parseJSONFileLBank(pair, ntrans, noData);
					else {
						System.out.println("Exchange not found");
						throw new NullPointerException();
					}
					z++;
				}
				noData = false;

				//terminate JSON file with final "}"
				try {
					Files.write(Paths.get(path+exchange+"_trades_"+pair+"_"+i+"-"+(month)+"-"+year+".json"), "}".getBytes(), StandardOpenOption.APPEND);
				}catch (IOException e) {
					e.printStackTrace();
				}

			}

			updateDataTrans(exchange, month, year, i, ntrans); //writing data acquired about the number of transactions for the day examined on a JSON file for the month selected

			//keep going on to next the next day
		}

	}
	
	@SuppressWarnings("unchecked")
	private static LinkedList<String> retrieveTradableAssets(LinkedList<String> pairs, String exchange) {
		String path = ".\\data\\"+exchange+"\\"; //must be present a data/exchange path
		URL u = null;
		try {
			if(exchange.equals("Kraken"))
				u = new URL("https://api.kraken.com/0/public/AssetPairs"); //create new URL object
			else if(exchange.equals("Binance"))
				u = new URL("https://api.binance.com/api/v3/ticker/price"); //create new URL object
			else if(exchange.equals("HitBTC"))
				u = new URL("https://api.hitbtc.com/api/2/public/ticker"); //create new URL object
			else if(exchange.equals("LBank"))
				u = new URL("https://api.lbkex.com/v1/ticker.do?symbol=all"); //create new URL object
			else {
				System.out.println("Exchange not found");
				throw new NullPointerException();
			}
			ReadableByteChannel rbc = Channels.newChannel(u.openStream()); //open Stream from the URL to receive data
			FileOutputStream fosTrades = new FileOutputStream(path+"tradablePairs_"+exchange+".json"); //open output stream
			fosTrades.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE); //writes on the output stream
			fosTrades.close(); //close output stream
		}
		catch (MalformedURLException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		
		
		try {
			/*parsing JSON file containing list of pairs*/
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(path+"tradablePairs_"+exchange+".json"));
			if(exchange.equals("Kraken")) {
				JSONObject jsonObject = (JSONObject) obj; //obtain JSON Object
				JSONObject result = (JSONObject) jsonObject.get("result"); //obtain result
				pairs = new LinkedList<String>(result.keySet());
			}
			else{
				pairs = new LinkedList<String>();
				JSONArray jsonArray = (JSONArray) obj; //obtain JSON Object
				Iterator<JSONObject> it = jsonArray.iterator();
				while(it.hasNext()) {
					JSONObject singlePair = it.next();
					pairs.add((String) singlePair.get("symbol"));
				}
			}
		}
		catch(FileNotFoundException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		catch(ParseException e) {e.printStackTrace();}
		
		return pairs;
	}
	
	private static long setInitialDate(int i, int month, int year, String exchange) {
		int dayOfMonth=i, hourOfDay=0, minute=0;

		Calendar startDate = new GregorianCalendar(year, month-1, dayOfMonth, hourOfDay, minute);
		long since = startDate.getTimeInMillis();
		if(exchange.equals("Kraken"))
			since = since * 1000000; //get time in nanoseconds (Kraken supports nanoseconds)
		return since;
	}
	
	private static long setFinalDate(int i, int month, int year, String exchange) {
		int dayOfMonth=i+1, hourOfDay=0, minute=0;

		Calendar endDate = new GregorianCalendar(year, month-1, dayOfMonth, hourOfDay, minute);
		long endTime = endDate.getTimeInMillis();
		if(exchange.equals("Kraken"))
			endTime = endTime * 1000000; //get time in nanoseconds (Kraken supports nanoseconds)
		return endTime;
	}
	
	@SuppressWarnings("unchecked")
	public static void updateDataTrans(String exchange, int month, int year, int i, long ntrans) {
		String path = ".\\data\\"+exchange+"\\"; //must be present a data/exchange path
		JSONParser parser = new JSONParser();
		Object obj = null;
		try {
			obj = parser.parse(new FileReader(path+(month)+"_"+exchange+".json"));
		} 
		catch (FileNotFoundException e1) { //if not found create new JSON file
			JSONObject newJSONObject = new JSONObject();
			try (FileWriter file = new FileWriter(path+(month)+"_"+exchange+".json")) { //create the new JSON file for the month
				file.write(newJSONObject.toJSONString());
			}
			catch(IOException e) {
				e.printStackTrace();
			}
		} 
		catch (IOException e1) {e1.printStackTrace();} 
		catch (ParseException e1) {e1.printStackTrace();}
		if(obj == null) {
			try {obj = parser.parse(new FileReader(path+(month)+"_"+exchange+".json"));} 
			catch (IOException | ParseException e) {e.printStackTrace();}
		}
		JSONObject jsonObject = (JSONObject) obj; //obtain JSON Object
		jsonObject.put(i, ntrans); //put the couple number_of_day_examined: number of transactions (e.g. "1": 15194 for the first day)

		try (FileWriter file = new FileWriter(path+(month)+"_"+exchange+".json")) { //create the new JSON file 
			file.write(jsonObject.toJSONString());
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	private static URL selectURL(String exchange, String pair, long since) {
		try {
			if(exchange.equals("Kraken"))
				return new URL("https://api.kraken.com/0/public/Trades?pair="+pair+"&since="+since); //create new URL object
			else if(exchange.equals("Binance"))
				return new URL("https://api.binance.com/api/v3/aggTrades?symbol="+pair+"&startTime="+since+"&endTime="+(since+700000)+"&limit=1000");  //create new URL object
			else if(exchange.equals("HitBTC"))
				return new URL("https://api.hitbtc.com/api/2/public/trades/"+pair+"?sort=ASC&from="+since+"&till="+(since+1800000)+"&limit=1000"); //create new URL object
			else if(exchange.equals("LBank"))
				return new URL("https://api.lbkex.com/v1/trades.do?symbol="+pair+"&size=600&time="+since); //create new URL object
			else {
				System.out.println("Exchange not found");
				throw new NullPointerException();
			}
		}
		catch (MalformedURLException e) {e.printStackTrace();}
		throw new NullPointerException();
	}
	
	private static long parseJSONFileKraken(String pair, long ntrans, boolean noData) {
		long since = 0;
		String path = ".\\data\\Kraken\\"; //must be present a data/exchange path
		try {
			//parsing tmp JSON file to extract the new time stamp needed
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(path+"tmp_Kraken.json"));
			JSONObject jsonObject = (JSONObject) obj; //obtain JSON Object


			JSONObject result = (JSONObject) jsonObject.get("result"); //obtain result
			JSONArray trans = (JSONArray) result.get(pair); //obtain pair selected
			ntrans = ntrans + trans.size(); //add number of transactions retrieved
			String lastStr = (String) result.get("last"); //retrieve "last" field which will be used as the next "since"

			if(since == (long) Long.parseLong(lastStr)) //same data
				noData = true;
			else
				since = (long) Long.parseLong(lastStr); //new time stamp used to make new API call
		}
		catch(FileNotFoundException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		catch(ParseException e) {e.printStackTrace();}
		return since;
	}
	
	private static long parseJSONFileBinance(String pair, long ntrans, boolean noData) {
		long since = 0;
		String path = ".\\data\\Binance\\"; //must be present a data/exchange path
		try {
			/*parsing JSON file*/
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(path+"tmp_Binance.json"));
			JSONArray jsonArray = (JSONArray) obj; //obtain JSON Object
			JSONObject result = null;
			//obtain last element..
			if(jsonArray.size() > 0) {
				result = (JSONObject) jsonArray.get(jsonArray.size()-1);
				ntrans = ntrans + jsonArray.size();
				if(since != (long) result.get("T"))
					since = (long) result.get("T");
				else
					noData = true;
			}
			else {
				noData = true;
			}
		}
		catch(FileNotFoundException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		catch(ParseException e) {e.printStackTrace();}
		return since;
	}
	
	private static long parseJSONFileHitBTC(String pair, long ntrans, boolean noData) {
		long since = 0;
		String path = ".\\data\\HitBTC\\"; //must be present a data/exchange path
		try {
			/*parsing JSON file*/
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(path+"tmp_HitBTC.json"));
			JSONArray jsonArray = (JSONArray) obj; //obtain JSON Object
			JSONObject result = null;
			//obtain last element..
			if(jsonArray.size() > 0) {
				result = (JSONObject) jsonArray.get(jsonArray.size()-1);
				ntrans = ntrans + jsonArray.size();
				String dateStr = (String) result.get("timestamp");
				ZonedDateTime date = ZonedDateTime.parse(dateStr);
				if(since != date.toInstant().toEpochMilli()) {
					since = date.toInstant().toEpochMilli();
				}
				else {
					since = since + 1800000; //every 30 minutes
				}
			}
			else {
				since = since + 1800000; //every 30 minutes
			}
		}
		catch(FileNotFoundException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		catch(ParseException e) {e.printStackTrace();}
		return since;
	}
	
	private static long parseJSONFileLBank(String pair, long ntrans, boolean noData) {
		long since = 0;
		String path = ".\\data\\LBank\\"; //must be present a data/exchange path
		
		try {
			/*parsing JSON file*/
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(path+"tmp_LBank.json"));
			JSONArray jsonArray = (JSONArray) obj; //obtain JSON Object
			JSONObject result = null;
			//obtain last element..
			if(jsonArray.size() > 0) {
				result = (JSONObject) jsonArray.get(jsonArray.size()-1);
				ntrans = ntrans + jsonArray.size();
				since = (long) result.get("date_ms");
			}
			else {
				noData = true;
			}
		}
		catch(FileNotFoundException e) {e.printStackTrace();}
		catch(IOException e) {e.printStackTrace();}
		catch(ParseException e) {e.printStackTrace();}
		return since;
	}
}
