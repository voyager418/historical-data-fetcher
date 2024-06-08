package src.main.java.historicaldatafetcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.ib.client.Contract;
import com.ib.client.Types;
import com.ib.controller.ApiController;
import com.opencsv.CSVWriter;

public class HistoricalDataHandler implements ApiController.IHistoricalDataHandler {
	private final CSVWriter writer;
	private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private String barTimestamp; // will always hold the most recent timestamp
	private boolean shouldFetchRemainingData;
	private final ApiController apiController;
	private final Types.BarSize barSize;
	private final Types.WhatToShow whatToShow;
	private Calendar from;
	private Calendar endDate;
	private Calendar to;
	private final Contract contract;

	public HistoricalDataHandler(ApiController apiController, Types.BarSize barSize, Types.WhatToShow whatToShow,
			Calendar from, Calendar to, Contract contract, boolean shouldFetchRemainingData) throws IOException {
		this.apiController = apiController;
		this.barSize = barSize;
		this.whatToShow = whatToShow;
		this.from = from;
		this.endDate = to;
		this.to = to;
		this.contract = contract;
		this.shouldFetchRemainingData = shouldFetchRemainingData;
		String fileName = this.getFileName(contract, barSize);
		File file = new File(fileName);
		FileWriter outputFile = new FileWriter(file);
		writer = new CSVWriter(outputFile);
		String[] header = { "timestamp", "open", "high", "low", "close", "volume" };
		writer.writeNext(header);
	}

	@Override
	public void historicalDataEnd() {
		if (!this.shouldFetchRemainingData ||
				getDifferenceOfDays(new Date(Timestamp.from(Instant.parse(barTimestamp)).getTime()), this.to.getTime()) <= 5) {
			try {
				writer.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			System.exit(0);
		}

		Timestamp lastFetchedTimestamp = Timestamp.from(Instant.parse(barTimestamp));
		int duration = getDifferenceOfDays(new Date(lastFetchedTimestamp.getTime()), new Date(System.currentTimeMillis()));
		if (duration > 356) {
			// year = year + 1
			endDate.set(endDate.getTime().getYear() + 1901, Calendar.JANUARY, 1, 0, 0, 0);
		} else {
			// current year
			this.shouldFetchRemainingData = false;
			endDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		}
		apiController.reqHistoricalData(this.contract,
				DATE_TIME_FORMAT.format(endDate.getTime()) + " UTC",
				duration > 365 ? 1 : duration,
				duration > 365 ? Types.DurationUnit.YEAR : Types.DurationUnit.DAY,
				this.barSize,
				whatToShow,
				true, false, this);
	}

	public void fetchCandlesticks() {
		int duration = getDifferenceOfDays(from.getTime(), to.getTime());
		duration = duration >= 365 ? 1 : duration;
		Types.DurationUnit durationUnit = Types.DurationUnit.DAY;
		if (duration == 1) {
			durationUnit = Types.DurationUnit.YEAR;
		}

		this.apiController.reqHistoricalData(contract,
				DATE_TIME_FORMAT.format(to.getTime()) + " UTC",
				duration,
				durationUnit,
				barSize,
				whatToShow,
				true, false, this);
	}

	public static int getDifferenceOfDays(Date d1, Date d2) {
		long diff = d2.getTime() - d1.getTime();
		return (int) TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}

	private String getFileName(Contract contract, Types.BarSize barSize) {
		return contract.symbol() + "-" + contract.currency() + "-" +
				contract.exchange() + "-" + barSize.toString().replace(" ", "") + ".csv";
	}

	@Override
	public void historicalData(com.ib.client.Bar bar) {
		try {
			Instant barInstant = DATE_TIME_FORMAT.parse(bar.time()).toInstant();
			barInstant = barInstant.plusSeconds(7200); // 2 hours to covert from UTC to New York time
			barTimestamp = barInstant.toString();
		} catch (ParseException e) {
			try {
				Date parsedDate = DATE_FORMAT.parse(bar.time());
				parsedDate.setHours(18);
				barTimestamp = parsedDate.toInstant().toString();
			} catch (ParseException e2) {
				throw new RuntimeException(e2);
			}
		}

		String[] data = { barTimestamp, String.valueOf(bar.open()), String.valueOf(bar.high()), String.valueOf(bar.low()),
				String.valueOf(bar.close()), String.valueOf(bar.volume()) };
		writer.writeNext(data);
	}
}
