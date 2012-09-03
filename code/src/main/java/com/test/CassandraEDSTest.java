package com.test;

import java.util.Random;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class CassandraEDSTest implements Runnable{

	static GigaSpace gigaspace ;
	static long MAX_WRITE = 5000;
	static long MAX_BATCH_WRITE = 500;
	public static void main(String[] args) throws Exception {

		gigaspace = new GigaSpaceConfigurer(new UrlSpaceConfigurer("jini://*/*/mySpace")).gigaSpace();
		Thread.sleep(2000);

		simpleTest();
		//documentTest();

		//complexTest();

	}


	static void complexTest() throws Exception
	{
		Thread tr[] = new Thread[4];
		for (int i=0;i<4;i++)
		{
			tr[i] = new Thread(new CassandraEDSTest());
		}
		for (int i=0;i<4;i++)
		{
			tr[i].start();
		}
		for (int i=0;i<4;i++)
		{
			tr[i].join();
		}
	}

	static void simpleTest()
	{
		MAX_WRITE  = 30;
		MAX_BATCH_WRITE = 2;
		new CassandraEDSTest().run();
	}

	static public void documentTest(){

		gigaspace.getTypeManager().registerTypeDescriptor(new SpaceTypeDescriptorBuilder("com.test.MyData").
				idProperty("id").
				addFixedProperty("id",Long.class).
				addFixedProperty("first",String.class).
				addFixedProperty("last",String.class).
				addFixedProperty("age",Integer.class).
				create()
				);
        gigaspace.getTypeManager().registerTypeDescriptor(new SpaceTypeDescriptorBuilder("com.test.SecondTestData").
        				idProperty("id").
        				addFixedProperty("id", Long.class).
        				addFixedProperty("info", String.class).
        				create()
        				);

		for (int i=21;i<31;i++)
		{
			SpaceDocument doc=new SpaceDocument("com.test.MyData");
			doc.setProperty("id", 10 + i);
			doc.setProperty("first","f" +i);
			doc.setProperty("last","l" +i);
			doc.setProperty("age",30 + i);
			gigaspace.write(doc);
		}

		for (int i=200;i<210;i++)
		{
			SpaceDocument doc=new SpaceDocument("com.test.MyData");
			doc.setProperty("id", 100 + i);
			doc.setProperty("first","first" +i);
			doc.setProperty("last","last" +i);
			doc.setProperty("age",30 + i);
			doc.setProperty("dynamicA" + i, "dynamicA" + i);
			doc.setProperty("dynamicB" + i, "dynamicB" + i);
			doc.setProperty("dynamicC" + i, "dynamicC" + i);
			gigaspace.write(doc, 120000);
        }

        for (int i = 21; i < 31; i++) {
            SpaceDocument doc = new SpaceDocument("com.test.SecondTestData");
            doc.setProperty("id", 10 + i);
            doc.setProperty("info", "info_" + i);
            gigaspace.write(doc);
        }

    }

	@Override
	public void run() {

		MyData o = new MyData();

		Random rand = new Random();
		long offset = Thread.currentThread().getId() * 100;

		// Testing Write
		System.out.println("Thread ID:" + Thread.currentThread().getId() + " Testing Write");
		for (long i=offset ;i<MAX_WRITE + offset ;i++)
		{
			o.setAge(10 + rand .nextInt(10));
			o.setFirst("first" + i);
			o.setLast("last" + i);
			o.setId(i);
			gigaspace.write(o, 120000);
		}

        // Testing Write SecondTestData
        System.out.println("Thread ID:" + Thread.currentThread().getId() + " Testing Write SecondTestData");
        for (long i = offset; i < MAX_WRITE + offset; i++) {
            SecondTestData secondTestData = new SecondTestData();
            secondTestData.setId(i);
            secondTestData.setInfo("info_" + i);
            gigaspace.write(secondTestData);
        }

		// Testing Update
		System.out.println("Thread ID:" + Thread.currentThread().getId() + " Testing Update");
		for (long i=offset;i<(MAX_WRITE / 10)+offset;i++)
		{
			o.setAge(10 + rand .nextInt(10));
			o.setFirst("firstXX" + i);
			o.setLast("lastYY" + i);
			o.setId(i);
			gigaspace.write(o);
		}

		// Testing Take
		System.out.println("Thread ID:" + Thread.currentThread().getId() + " Testing Take");
		o = new MyData();
		for (long i=offset+10;i<offset+20;i++)
		{
			o.setId(i);
			gigaspace.take(o);
		}

		// Testing WriteMultiple/UpdateMultiple
		System.out.println("Thread ID:" + Thread.currentThread().getId() + " Testing WriteMultiple/UpdateMultiple");
		for (int j=0;j<MAX_BATCH_WRITE ;j++)
		{
			MyData arry[] = new MyData[10];
			int count = 0;
			for (long i=offset+1000;i<offset+1010;i++)
			{
				arry[count]  = new MyData();
				arry[count].setAge(10 + rand .nextInt(10));
				arry[count].setFirst("first" + i);
				arry[count].setLast("last" + i);
				arry[count].setId(i);
				count++;
			}
			gigaspace.writeMultiple(arry);
		}
	}
}
