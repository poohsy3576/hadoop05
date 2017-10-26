package com.example.calc;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CalculatorTest {

	// CalculatorTest 시작 최초에 한번 호출
	@BeforeClass
	public static void setUpBeforeClass() throws Exception { System.out.println("##### setUpBeforeClass()...");	}

	// CalculatorTest 종료 최초에 한번 호출
	@AfterClass
	public static void tearDownAfterClass() throws Exception { System.out.println("##### tearDownAfterClass()...");	}

	// testXXX method 시작할때마다 호출
	@Before
	public void setUp() throws Exception {	System.out.println("@@@ setUp()...");	}

	// testXXX method 종료할때마다 호출
	@After
	public void tearDown() throws Exception {	System.out.println("@@@ tearDown()...");	}

	@Test
	public void testAdd() {
		System.out.println("** testAdd()...");
		
		assertEquals(10, Calculator.add(5, 5));
	}

	@Test
	public void testSubstract() {
		System.out.println("** testSubstract()...");
		
		assertThat(Calculator.substract(10, 5), is(5));
	}

	@Test
	public void testMultiply() {
		System.out.println("** testMultiply()...");
		
		assertThat(Calculator.multiply(10, 5), is(equalTo(50)));
	}

	@Test
	public void testDivide() {
		System.out.println("** testDivide()...");

		assertThat(Calculator.divide(10, 5), is(equalTo(2)));
	}

	@Test
	public void testMod() {
		System.out.println("** testMod()...");
		
		assertThat(Calculator.mod(10, 5), is(equalTo(0)));
	}

}
