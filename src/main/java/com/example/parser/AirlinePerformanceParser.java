package com.example.parser;

import org.apache.hadoop.io.Text;

//외부의 데이터를 자바가 다루기 쉽도록 class화 시키는 과정
public class AirlinePerformanceParser {

	// 필수 입력 항목
	private int year;
	private int month;
	private String uniqueCarrier;

	// data가 null일 수도 있다.
	private int arriveDelayTime;
	private int departureDelayTime;
	private int distance;

	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;

	// parsing 작업
	public AirlinePerformanceParser(Text text) {

		String[] columns = text.toString().split(",");

		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);
		uniqueCarrier = columns[8];

		// depdelay parsing
		if (!columns[15].equals("NA")) {
			departureDelayTime = Integer.parseInt(columns[15]);
		} else {
			departureDelayAvailable = false;
		}

		// arrdelay parsing
		if (!columns[14].equals("NA")) {
			arriveDelayTime = Integer.parseInt(columns[14]);
		} else {
			arriveDelayAvailable = false;
		}

		// distance parsing
		if (!columns[18].equals("NA")) {
			distance = Integer.parseInt(columns[18]);
		} else {
			distanceAvailable = false;
		}
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public void setArriveDelayAvailable(boolean arriveDelayAvailable) {
		this.arriveDelayAvailable = arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public void setDepartureDelayAvailable(boolean departureDelayAvailable) {
		this.departureDelayAvailable = departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public void setDistanceAvailable(boolean distanceAvailable) {
		this.distanceAvailable = distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

}
