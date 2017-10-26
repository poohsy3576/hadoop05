package com.example.parser;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import static org.hamcrest.CoreMatchers.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AirlinePerformanceParserTest {

	//Test 전에 실행된다. ==> 초기화 작업
	@Before
	public void setUp() throws Exception {
		System.out.println("##########");
		System.out.println("setUp()...");
		System.out.println("##########");
	}
	
	//Test 후에 실행된다.
	@After
	public void tearDown() throws Exception {
		System.out.println("#############");
		System.out.println("tearDown()...");
		System.out.println("#############");
	}
	String string  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String string2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Test
	public void testConstructor() {
		System.out.println("testConstructor()...");
		
		// 2008/1/3		uniqueCarrier = WN		arrdelay = 34		depdelay = 34		distance = 515
		Text text = new Text(string);
		
		/*
		 * year = 2008
		 * month = 1
		 * arrDelay = 34, arriveAvailable = true
		 * depDelay = 34, departureAvailable = true
		 * distance = 515, distanceAvailable = true
		 */
		AirlinePerformanceParser parser = new AirlinePerformanceParser(text);
		
		//예상되는 값(2008)과 실제 값(parser.getYear()) 비교
		//출력 결과를 보지 않고도 에러가 있는지 확인할 수 있다.
		assertEquals(2008, parser.getYear());
		
		
		//CoreMatchers static method를 마치 내것인냥 쓰기 위해 import시에 static을 넣어주면 된다.
//		assertThat(2008, CoreMatchers.is(CoreMatchers.equalTo(parser.getYear())));
		assertThat(2008, is(equalTo(parser.getYear())));

		assertEquals(1, parser.getMonth());
		assertEquals(34, parser.getArriveDelayTime());
		assertEquals(34, parser.getDepartureDelayTime());
		assertEquals(515, parser.getDistance());
		
		
		assertTrue(parser.isArriveDelayAvailable());
		assertTrue(parser.isDepartureDelayAvailable());
		assertTrue(parser.isDistanceAvailable());
		
		System.out.println("year = " + parser.getYear());
		System.out.println("month = " + parser.getMonth());
		System.out.println("arrDelay = " + parser.getArriveDelayTime());
		System.out.println("arriveAvailale = " + parser.isArriveDelayAvailable());
		System.out.println("depDelay = " + parser.getDepartureDelayTime());
		System.out.println("departureAvailable = " + parser.isDepartureDelayAvailable());
		System.out.println("distance = " + parser.getDistance());
		System.out.println("distanceAvailable = " + parser.isDistanceAvailable());
	}
	
	@Test
	public void testConstructor2() {
		System.out.println("testConstructor2()...");
		AirlinePerformanceParser parser = new AirlinePerformanceParser(new Text(string2));
		
		assertEquals(2008, parser.getYear());
		assertEquals(1, parser.getMonth());
		
		assertFalse(parser.isArriveDelayAvailable());
		assertFalse(parser.isDepartureDelayAvailable());
		assertFalse(parser.isDistanceAvailable());
	}

}
