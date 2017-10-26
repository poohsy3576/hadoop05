package chap05;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class RandomTest {

	@Test
	public void test() {
		Random r = new Random();
		int rint = r.nextInt(10);
		System.out.println("rint = " + rint);
	}

	@Test
	public void testMaxInteger() {
		System.out.println("max = " + Integer.MAX_VALUE);
	}
	
	@Test
	public void testMaxIntegerGenerator() {
		Random r = new Random();
		
		for (int i=0; i<100; i++) {
			String strInt = String.format("%010d", r.nextInt(Integer.MAX_VALUE));
			System.out.println(strInt);
		}
	}
}
