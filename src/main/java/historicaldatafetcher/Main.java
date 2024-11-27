package src.main.java.historicaldatafetcher;

import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import com.ib.client.Contract;
import com.ib.client.Types;
import com.ib.controller.ApiConnection;
import com.ib.controller.ApiController;


public class Main {

	public static void main(String[] args) throws Exception {
		ApiController apiController = new ApiController(new ConnectionHandler(), new InLogger(), new OutLogger());
		apiController.connect("127.0.0.1", 4000, 3, "");

		Contract contract = spy();
		Calendar from = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		from.set(2024, Calendar.OCTOBER, 1, 0, 0, 0);
		Calendar to = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		to.set(2024, Calendar.NOVEMBER, 1, 0, 0, 0); // non inclusive

		HistoricalDataHandler historicalDataHandler = new HistoricalDataHandler(
				apiController,
				Types.BarSize._1_min,
				Types.WhatToShow.TRADES, from, to,
				contract,
				true);
		historicalDataHandler.fetchCandlesticks();
	}

	public static Contract spx() {
		//! [cashcontract]
		Contract contract = new Contract();
		contract.symbol("SPX");
		contract.secType("IND");
		contract.currency("USD");
		contract.exchange("CBOE");
		//! [cashcontract]
		return contract;
	}

	public static Contract spy() {
		//! [stkcontract]
		Contract contract = new Contract();
		contract.symbol("SPY");
		contract.secType("STK");
		contract.currency("USD");
		contract.exchange("ARCA");
		//! [stkcontract]
		return contract;
	}
}

class InLogger implements ApiConnection.ILogger {
	@Override
	public void log(String valueOf) {
		System.out.println("In message: " + valueOf);
	}
}

class OutLogger implements ApiConnection.ILogger {
	@Override
	public void log(String valueOf) {
		System.out.println("Out message: " + valueOf);
	}
}

class ConnectionHandler implements ApiController.IConnectionHandler {

	@Override
	public void connected() {

	}

	@Override
	public void disconnected() {

	}

	@Override
	public void accountList(List<String> list) {

	}

	@Override
	public void error(Exception e) {
		System.out.println("Error occurred: " + e.getMessage());
	}

	@Override
	public void message(int id, int errorCode, String errorMsg, String advancedOrderRejectJson) {
		System.out.println("Got message: " + errorMsg);
	}

	@Override
	public void show(String string) {
		System.out.println("Showing:" + string);
	}
}
