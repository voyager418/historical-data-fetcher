package src.main.java.historicaldatafetcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.ib.client.Contract;
import com.ib.client.Types;
import com.ib.controller.ApiController;
import com.ib.controller.Bar;
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
	private final Map<String, Integer> hoursToAddForDate = new HashMap<>(); // to convert from UTC to New York time starting always at 9h30
	private final Set<String> fetchedTimestamps = new HashSet<>();
	private Instant previousInstant = Instant.MAX;
	private Bar previousBar = null;
	private LocalTime previousTime = null;

	public HistoricalDataHandler(ApiController apiController, Types.BarSize barSize, Types.WhatToShow whatToShow,
			Calendar from, Calendar to, Contract contract, boolean shouldFetchRemainingData) throws IOException {
		this.apiController = apiController;
		this.barSize = barSize;
		this.whatToShow = whatToShow;
		this.from = from;
		this.endDate = from;
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

		// continue fetching remaining
		Timestamp lastFetchedTimestamp = Timestamp.from(Instant.parse(barTimestamp));
		int duration = getDifferenceOfDays(new Date(lastFetchedTimestamp.getTime()), new Date(System.currentTimeMillis()));
		if (duration > 356) {
			// year = year + 1
			endDate.set(endDate.getTime().getYear() + 1901, Calendar.JANUARY, 1, 0, 0, 0);
		} else {
			// current year
			this.shouldFetchRemainingData = false;
			endDate.set(to.getTime().getYear() + 1900, to.getTime().getMonth(), to.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
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
		int duration = getDifferenceOfDays(from.getTime(), to.getTime()) - 2;
		duration = duration >= 365 ? 1 : duration;
		Types.DurationUnit durationUnit = Types.DurationUnit.DAY;
		endDate = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		endDate.set(to.getTime().getYear() + 1900, to.getTime().getMonth(), to.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
		endDate.add(Calendar.DAY_OF_MONTH, 1);
		if (duration == 1) {
			durationUnit = Types.DurationUnit.YEAR;
			endDate.set(from.getTime().getYear() + 1901, Calendar.JANUARY, 1, 0, 0, 0);
		}

		this.apiController.reqHistoricalData(contract,
				DATE_TIME_FORMAT.format(endDate.getTime()) + " UTC", // format is 20241031 15:59:00 UTC
				duration,
				durationUnit,
				barSize,
				whatToShow,
				true, false, this);
	}

	public static int getDifferenceOfDays(Date d1, Date d2) {
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();

		if (d1.after(d2)) {
			// Swap dates if d1 is after d2
			Date temp = d1;
			d1 = d2;
			d2 = temp;
		}

		startCal.setTime(d1);
		endCal.setTime(d2);

		int workingDays = 0;

		// Iterate through each day in the range
		while (startCal.before(endCal) || startCal.equals(endCal)) {
			int dayOfWeek = startCal.get(Calendar.DAY_OF_WEEK);
			if (dayOfWeek != Calendar.SATURDAY && dayOfWeek != Calendar.SUNDAY) {
				workingDays++;
			}
			// Move to the next day
			startCal.add(Calendar.DAY_OF_MONTH, 1);
		}

		return workingDays ;
	}

	private String getFileName(Contract contract, Types.BarSize barSize) {
		return contract.symbol() + "-" + contract.currency() + "-" +
				contract.exchange() + "-" + barSize.toString().replace(" ", "") + ".csv";
	}

	@Override
	public void historicalData(Bar bar) {
		Instant barInstant = null;
		try {
			// the time is either between 6h30 -> 13h or 7h30 -> 14h
			barInstant = DATE_TIME_FORMAT.parse(bar.timeStr()).toInstant();
			String day = barInstant.toString().split("T")[0];
            if (!Objects.equals(contract.symbol(), "VIX")) {
                if (!hoursToAddForDate.containsKey(day)) {
                    if (barInstant.toString().contains("T06:")) {
                        hoursToAddForDate.put(day, 3);
                    } else if (barInstant.toString().contains("T07:")) {
                        hoursToAddForDate.put(day, 2);
                    } else if (barInstant.toString().contains("T08:")) {
                        hoursToAddForDate.put(day, 1);
                    } else {
                        hoursToAddForDate.put(day, 0);
                    }
                }
            } else {
				// for vix (hours are between 3:15AM and 16h)
				if (!hoursToAddForDate.containsKey(day)) {
					if (barInstant.toString().contains("T00:15")) {
						hoursToAddForDate.put(day, 3);
					} else if (barInstant.toString().contains("T01:15")) {
						hoursToAddForDate.put(day, 2);
					} else if (barInstant.toString().contains("T02:15")) {
						hoursToAddForDate.put(day, 1);
					} else {
						hoursToAddForDate.put(day, 0);
					}
				}
			}

			barInstant = barInstant.plusSeconds(TimeUnit.HOURS.toSeconds(hoursToAddForDate.get(day)));
			barTimestamp = barInstant.toString();
		} catch (ParseException e) { // this is normal and happens if we fetch for daily candlesticks
			try {
				Date parsedDate = DATE_FORMAT.parse(bar.timeStr());
				parsedDate.setHours(18);
				barTimestamp = parsedDate.toInstant().toString();
			} catch (ParseException e2) {
				throw new RuntimeException(e2);
			}
		}

		if (Objects.equals(contract.symbol(), "VIX")) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			LocalTime time = LocalDateTime.parse(barTimestamp, formatter).toLocalTime();
			if (time.isAfter(LocalTime.of(15, 59))) {
				return;
			}
			if (time.isBefore(LocalTime.of(9, 30))) {
				return;
			}
			previousTime = time;
			// to fill beginning missing data (problem with IBKR not with us)
			LocalTime tempTime = LocalTime.of(9, 30);
			boolean prev = false;
			while (!tempTime.equals(time)) {
				String timestamp = getTimestamp(barInstant, tempTime);
				if (!fetchedTimestamps.contains(timestamp)) {
					String[] data = { timestamp, String.valueOf(bar.open()), String.valueOf(bar.high()), String.valueOf(bar.low()),
							String.valueOf(bar.close()), String.valueOf(bar.volume()) };
					if (fetchedTimestamps.add(timestamp)) { // to avoid writing duplicates
						writer.writeNext(data);
					}
					prev = true;
				}
				tempTime = tempTime.plusMinutes(1);
			}
			// to fill ending missing data
			if (!prev && previousInstant != Instant.MAX && !barInstant.atZone(ZoneId.systemDefault()).toLocalDate().equals(
				previousInstant.atZone(ZoneId.systemDefault()).toLocalDate()) &&
				(previousTime.getHour() != 15 || previousTime.getMinute() != 59)) {
				tempTime = LocalTime.of(previousTime.getHour(), previousTime.getMinute());
				tempTime = tempTime.plusMinutes(1);
				while (!tempTime.equals(LocalTime.of(16, 0))) {
					String timestamp = getTimestamp(previousInstant, tempTime);
					if (!fetchedTimestamps.contains(timestamp)) {
						String[] data = { timestamp, String.valueOf(previousBar.open()), String.valueOf(previousBar.high()), String.valueOf(previousBar.low()),
								String.valueOf(previousBar.close()), String.valueOf(previousBar.volume()) };
						if (fetchedTimestamps.add(timestamp)) { // to avoid writing duplicates
							writer.writeNext(data);
						}
					}
					tempTime = tempTime.plusMinutes(1);
				}
			}
		}
		String[] data = { barTimestamp, String.valueOf(bar.open()), String.valueOf(bar.high()), String.valueOf(bar.low()),
				String.valueOf(bar.close()), String.valueOf(bar.volume()) };

		if (fetchedTimestamps.add(barTimestamp)) { // to avoid writing duplicates
			writer.writeNext(data);
		}
		previousInstant = barInstant;
		previousBar = bar;
	}

	private String getTimestamp(Instant instant, LocalTime time) {
		LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();

		// Combine LocalDate and LocalTime to form LocalDateTime
		LocalDateTime dateTime = LocalDateTime.of(localDate, time);

		// Define formatter for barTimestamp format
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

		// Format as barTimestamp string
		return dateTime.format(formatter);
	}
}
