package my.proj;

import org.apache.beam.sdk.annotations.Experimental;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.lang.Integer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.lang.Double;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.GcpCredentialFactory;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.BeamSql.SimpleQueryTransform;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdks.java.extensions.sql.repackaged.org.apache.calcite.interpreter.Row;
import org.apache.beam.sdks.java.extensions.sql.repackaged.org.apache.calcite.schema.Schema;
import org.apache.calcite.util.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.sl.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.appengine.api.blobstore.dev.BlobStorage;
import com.google.appengine.api.blobstore.dev.BlobStorageFactory;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.ListOptions;
import com.google.appengine.tools.cloudstorage.ListResult;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.util.Logging;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.bigtable.admin.v2.StorageType;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.pojo.ClassBrands;
import com.pojo.ClassWeeklyDueto;
import com.pojo.ClassWeeks;
import com.pojo.Composite_events;
import com.pojo.Durationflag;
import com.pojo.Hispanic6temp;
import com.pojo.ClassPeriod;
import com.pojo.ClassEvents;
import com.pojo.ClassShip;
import com.pojo.ClassFinance;
import com.pojo.ClassSpend;
import com.pojo.ClassCurves;
import com.pojo.ClassProduct;
import com.pojo.ClassBasis;
import com.pojo.ClassEvent1;
import com.pojo.MyOutputClass;
import com.pojo.VehicleGamma;
import com.pojo.ClassBrandMap1;
import com.pojo.CampaignGamma;
import com.google.appengine.api.utils.*;
import com.pojo.ClassWeekend;
import com.pojo.ClassEventExec;
import com.pojo.ClassHispanic;
import com.pojo.ClassEfficiency;
import com.pojo.ClassDuration;

public class StarterPipeline {
	static String filename_WeeklyD;
	static ArrayList<String> al = new ArrayList<String>();

	public static class AddS implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static String eval(String input,Integer i) throws ParseException {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			System.out.println("dateformat : " + dateFormat);

			// String strdate = Long.toString(input);

			Date date1 = dateFormat.parse(input);

			Calendar c = Calendar.getInstance();

			c.setTime(date1);

			c.add(Calendar.DATE, i);

			String f = c.getTime().toString();

			System.out.println(f);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH);
			int a = month + 1;
			String t = null;
			if (a < 10) {
				t = "0" + Integer.toString(a);

			} else {
				t = Integer.toString(a);
			}
			int day = c.get(Calendar.DAY_OF_MONTH);
			String r = null;
			if (day < 10) {
				r = "0" + Integer.toString(day);

			} else {
				r = Integer.toString(day);
			}
			System.out.println(year);
			System.out.println(t);
			System.out.println(r);
			String ddate = Integer.toString(year) + t + r;
			return ddate;

		}
	}

	public static class AddF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static String eval(String input) throws ParseException {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyymmdd");
			System.out.println("dateformat : " + dateFormat);

			// String strdate = Long.toString(input);

			Date date1 = dateFormat.parse(input);

			Calendar c = Calendar.getInstance();

			c.setTime(date1);

			c.add(Calendar.DATE, 14);

			String f = c.getTime().toString();

			System.out.println(f);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH);
			int a = month + 1;
			String t = null;
			if (a < 10) {
				t = "0" + Integer.toString(a);

			} else {
				t = Integer.toString(a);
			}
			int day = c.get(Calendar.DAY_OF_MONTH);
			String r = null;
			if (day < 10) {
				r = "0" + Integer.toString(day);

			} else {
				r = Integer.toString(day);
			}
			System.out.println(year);
			System.out.println(t);
			System.out.println(r);
			String ddate = Integer.toString(year) + t + r;
			return ddate;

		}
	}

	public static class AddT implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static String eval(String input) throws ParseException {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			System.out.println("dateformat : " + dateFormat);

			// String strdate = Long.toString(input);

			Date date1 = dateFormat.parse(input);

			Calendar c = Calendar.getInstance();

			c.setTime(date1);

			c.add(Calendar.DATE, 21);

			String f = c.getTime().toString();

			System.out.println(f);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH);
			int a = month + 1;
			String t = null;
			if (a < 10) {
				t = "0" + Integer.toString(a);

			} else {
				t = Integer.toString(a);
			}
			int day = c.get(Calendar.DAY_OF_MONTH);
			String r = null;
			if (day < 10) {
				r = "0" + Integer.toString(day);

			} else {
				r = Integer.toString(day);
			}
			System.out.println(year);
			System.out.println(t);
			System.out.println(r);
			String ddate = Integer.toString(year) + t + r;
			return ddate;

		}
	}

	public static class AddW implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static String eval(String input) throws ParseException {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			System.out.println("dateformat : " + dateFormat);

			// String strdate = Long.toString(input);

			Date date1 = dateFormat.parse(input);

			Calendar c = Calendar.getInstance();

			c.setTime(date1);

			c.add(Calendar.DATE, 28);

			String f = c.getTime().toString();

			System.out.println(f);
			int year = c.get(Calendar.YEAR);
			int month = c.get(Calendar.MONTH);
			int a = month + 1;
			String t = null;
			if (a < 10) {
				t = "0" + Integer.toString(a);

			} else {
				t = Integer.toString(a);
			}
			int day = c.get(Calendar.DAY_OF_MONTH);
			String r = null;
			if (day < 10) {
				r = "0" + Integer.toString(day);

			} else {
				r = Integer.toString(day);
			}
			System.out.println(year);
			System.out.println(t);
			System.out.println(r);
			String ddate = Integer.toString(year) + t + r;
			return ddate;

		}
	}
	
	public static class durationUDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static Double eval(Double a,Double b,Double c,Double d,Double e,Double f) throws ParseException {
			Double g = (double) 0;
			if(b!=0.0 && c!=0.0 && d!=0.0 && e!=0.0 && f!=0.0 ) {
				g = a/b/c*d*b/e/f;
			}else {
				g = 0.0 ;
			}
			return g;
		}
	}

	public static class X2UDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static Double eval(Double a,Double b,Double c) throws ParseException {
			Double g = (double) 0;
			if(b!=0 && c!=0) {
				g = a/b/c;
			}else {
				g = 0.0 ;
			}
			return g;
		}
	}
	
	public static class GAMMAUDF implements BeamSqlUdf {
	private static final long serialVersionUID = 1L;

	public static Double eval(Double a,Double b,Double c,Double d,Double e,Double f) throws ParseException {
		Double g = (double) 0;
		if(b!=0 && c!=0 && d!=0 && e!=0 && f!=0) {
			g = a/b*c*d/e/f;
		}else {
			g = 0.0 ;
		}
		return g;
	}
}
	
	public static class hispanicUDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static Double eval(String a,Double b,Double c) throws ParseException {
			Double g = 0.0;
			if(a==null || a == "") {
				g = b;
			}else{
				if(c == null) {
					g = 0.0;
				}else {
					g = c;
				}	
			}
			return g;
		}
	}
	
	public static class rec_19_testUDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static Double eval(Double a,Double b) throws ParseException {
			Double g = (double) 0;
			if(b == 0.0) {
				g = 0.0;
			}else {
				g = a/b*100 ;
			}
			return g;
		
		}
	}
	
	public static class contUDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;

		public static Double eval(Double a,Double b,Double c) throws ParseException {
			Double g = (double) 0;
			if(a/b <= c) {
				g = a/b;
			}else {
				g = c ;
			}
			return g;
		}
	}
	
	public static class duration_flag_UDF implements BeamSqlUdf {
		private static final long serialVersionUID = 1L;
		public static Double eval(Double a) throws ParseException {
			Double g = (double) 0;
			if(a == null) {
				g = 0.0;
			}else {
				g = a ;
			}
			return g;
		}
		
	}
	
	// function to convert Event xls to csv
	@SuppressWarnings("deprecation")
	public static void printBlob(com.google.cloud.storage.Storage storage, String bucketName, String blobPath)
			throws IOException, InvalidFormatException, EncryptedDocumentException,
			org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		try (ReadChannel reader = ((com.google.cloud.storage.Storage) storage).reader(bucketName, blobPath)) {
			// InputStream inputStream = Channels.newInputStream(reader);
			BufferedInputStream bis = new BufferedInputStream(Channels.newInputStream(reader));
			Workbook wb = WorkbookFactory.create(bis);
			StringBuffer data = new StringBuffer();
			for (int i = 0; i < wb.getNumberOfSheets(); i++) {
				String fName = wb.getSheetAt(i).getSheetName();
				HSSFSheet sheet = (HSSFSheet) wb.getSheetAt(i);
				Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = sheet.iterator();
				data.delete(0, data.length());
				while (rowIterator.hasNext()) {
					// Get Each Row
					org.apache.poi.ss.usermodel.Row row = rowIterator.next();
					data.append('\n');
					// Iterating through Each column of Each Row
					Iterator<Cell> cellIterator = row.cellIterator();

					while (cellIterator.hasNext()) {
						Cell cell = cellIterator.next();

						// Checking the cell format
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_NUMERIC:
							data.append(cell.getNumericCellValue() + ",");
							break;
						case Cell.CELL_TYPE_STRING:
							data.append(cell.getStringCellValue() + ",");
							break;
						case Cell.CELL_TYPE_BOOLEAN:
							data.append(cell.getBooleanCellValue() + ",");
							break;
						case Cell.CELL_TYPE_BLANK:
							data.append("" + ",");
							break;
						default:
							data.append(cell + ",");
						}
					}
				}

				String filename = "final_input/Events/" + fName;
				BlobId blobId = BlobId.of("cloroxtegadeff", filename);
				byte[] content = data.toString().getBytes(UTF_8);
				BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
				try (WriteChannel writer = storage.writer(blobInfo)) {
					writer.write(ByteBuffer.wrap(content, 0, content.length));
				}
			}
		}
	}

	// main method
	public static void main(String[] args)
			throws IOException, EncryptedDocumentException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		SimpleDateFormat date1 = new SimpleDateFormat("MM/dd/yy");
		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		options.setProject("ad-efficiency-dev");
		options.setStagingLocation("gs://cloroxtegadeff/staging");
		// options.setGcpTempLocation("gs://cloroxtegbucket/tmp");
		options.setRunner(DataflowRunner.class);
		DataflowRunner.fromOptions(options);
		Pipeline p = Pipeline.create(options);

		// Function call to convert excel to csv
		com.google.cloud.storage.Storage storage = StorageOptions.getDefaultInstance().getService();
		String bucketName = "cloroxtegadeff";
		String blobPath = "final_input/Event_Setup_CLOROX.xls";

		try {
			printBlob(storage, bucketName, blobPath);
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		}

		// implement CompositeEvents.csv
		PCollection<String> weekend_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Weekend.csv"));
		PCollection<ClassWeekend> pojos100 = weekend_read.apply(ParDo.of(new DoFn<String, ClassWeekend>() { // converting
																											// String
																											// into
																											// class
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws IOException, ParseException {

				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (header.contains("EveDate"))
					System.out.println("Header");
				else {
					ClassWeekend cww = new ClassWeekend();
					cww.setEveDate(date1.parse(strArr[0]));
					c.output(cww);
				}
			}
		}));

		List<String> fieldNames15 = Arrays.asList("EveDate");
		List<Integer> fieldTypes15 = Arrays.asList(Types.DATE);
		final BeamRecordSqlType appType15 = BeamRecordSqlType.create(fieldNames15, fieldTypes15);

		PCollection<BeamRecord> apps15 = pojos100.apply(ParDo.of(new DoFn<ClassWeekend, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType15, c.element().EveDate);
				c.output(br);
			}
		})).setCoder(appType15.getRecordCoder());
		PCollection<BeamRecord> Wknd = apps15.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());

		// implement Financial.csv
		PCollection<String> financeobj = p
				.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Financials123.csv"));
		PCollection<ClassFinance> pojos5 = financeobj.apply(ParDo.of(new DoFn<String, ClassFinance>() { // converting
																										// String into
																										// classtype
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr2 = c.element().split(",",-1);
				String header = Arrays.toString(strArr2);
				ClassFinance fin = new ClassFinance();

				if (header.contains("Beneficiary"))
					System.out.println("Header");
				else {
					fin.setBeneficiaryFinance(strArr2[0].trim());
					fin.setCatlibCode(strArr2[1].trim());
					if(strArr2[2].isEmpty()) {
						fin.setrNR(0.00);
					}else {
						fin.setrNR(Double.valueOf(strArr2[2]));
					}
					
					if(strArr2[3].isEmpty()) {
						fin.setrNCS(0.00);
					}else {
						fin.setrNCS(Double.valueOf(strArr2[3]));
					}
					if(strArr2[4].isEmpty()) {
						fin.setrCtb(0.00);
					}else {
						fin.setrCtb(Double.valueOf(strArr2[4]));
					}
					if(strArr2[5].isEmpty()) {
						fin.setrAC(0.00);
					}else {
						fin.setrAC(Double.valueOf(strArr2[5]));
					}
					c.output(fin);
				}
			}
		}));

		List<String> fieldNames5 = Arrays.asList("BeneficiaryFinance", "CatlibCode", "rNR", "rNCS", "rCtb", "rAC");
		List<Integer> fieldTypes5 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE);
		final BeamRecordSqlType appType5 = BeamRecordSqlType.create(fieldNames5, fieldTypes5);

		PCollection<BeamRecord> apps5 = pojos5.apply(ParDo.of(new DoFn<ClassFinance, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType5, c.element().BeneficiaryFinance, c.element().CatlibCode,
						c.element().rNR, c.element().rNCS, c.element().rCtb, c.element().rAC);
				c.output(br);
			}
		})).setCoder(appType5.getRecordCoder());
		PCollection<BeamRecord> financials = apps5.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
		


//		// implement ReachCurves.csv
		PCollection<String> curveobj = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/ReachCurves.csv"));
		PCollection<ClassCurves> pojos8 = curveobj.apply(ParDo.of(new DoFn<String, ClassCurves>() { // converting String
																									// into class type

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (header.contains("Alpha"))
					System.out.println("Header");
				else {
					ClassCurves curves = new ClassCurves();
					curves.setBrand(strArr[0].trim());
					curves.setSBU(strArr[1].trim());
					curves.setDivision(strArr[2].trim());
					curves.setAlpha(Double.parseDouble(strArr[3].trim()));
					curves.setBeta(Double.parseDouble(strArr[4].trim()));
					curves.setMarket(strArr[5].trim());
					curves.setSubChannel(strArr[6].trim());
					c.output(curves);
				}
			}
		}));

		List<String> fieldNames8 = Arrays.asList("Brand", "SBU", "Division", "Alpha", "Beta", "Market", "SubChannel");
		List<Integer> fieldTypes8 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE,
				Types.DOUBLE, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType8 = BeamRecordSqlType.create(fieldNames8, fieldTypes8);

		PCollection<BeamRecord> apps8 = pojos8.apply(ParDo.of(new DoFn<ClassCurves, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType8, c.element().Brand, c.element().SBU, c.element().Division,
						c.element().Alpha, c.element().Beta, c.element().Market, c.element().SubChannel);
				c.output(br);
			}
		})).setCoder(appType8.getRecordCoder());
		PCollection<BeamRecord> curve = apps8.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
//

//		// implement Shipment.csv
		PCollection<String> ship_read = p
				.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/ShipmentInput.csv"));
		PCollection<ClassShip> pojos4 = ship_read.apply(ParDo.of(new DoFn<String, ClassShip>() { // converting String
																									// into classType

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {

				String[] strArr = c.element().split(",",-1);
				String header = Arrays.toString(strArr);
				if (header.contains("Projection Factor"))
					System.out.println("Header");
				else {
					ClassShip ship = new ClassShip();
					ship.setBrand(strArr[0].trim());
					ship.setBeneficiary(strArr[1].trim());
					ship.setCatlib(strArr[2].trim());
					ship.setChannel(strArr[3].trim());
					ship.setPeriod(strArr[4].trim());
					if(strArr[5].isEmpty()) {
						ship.setChannelVolume(0.00);
					}else {
						ship.setChannelVolume(Double.parseDouble(strArr[5].trim()));
					}
					
					if(strArr[6].isEmpty()) {
						ship.setAllOutletVolume(0.00);
					}else {
						ship.setAllOutletVolume(Double.parseDouble(strArr[6].trim()));
					}
					if(strArr[7].isEmpty()) {
						ship.setProjectionFactor(0.00);
					}else {
						ship.setProjectionFactor(Double.parseDouble(strArr[7].trim()));
					}
					c.output(ship);
				}
			}
		}));

		List<String> fieldNames4 = Arrays.asList("Brand", "Beneficiary", "Catlib", "Channel", "Period", "ChannelVolume",
				"AllOutletVolume", "ProjectionFactor");
		List<Integer> fieldTypes4 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE);
		final BeamRecordSqlType appType4 = BeamRecordSqlType.create(fieldNames4, fieldTypes4);

		PCollection<BeamRecord> apps4 = pojos4.apply(ParDo.of(new DoFn<ClassShip, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType4, c.element().Brand, c.element().Beneficiary, c.element().Catlib,
						c.element().Channel, c.element().Period, c.element().ChannelVolume, c.element().AllOutletVolume,
						c.element().ProjectionFactor);
				c.output(br);
			}
		})).setCoder(appType4.getRecordCoder());
		PCollection<BeamRecord> shipment = apps4.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());

		
		// implement Period.csv
		PCollection<String> period_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Period.csv"));
		PCollection<ClassPeriod> pojos6 = period_read.apply(ParDo.of(new DoFn<String, ClassPeriod>() { // converting
																										// String into
																										// classtype

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (header.contains("Source_BDA"))
					System.out.println("Header");
				else {
					ClassPeriod per = new ClassPeriod();
					per.setSource_BDA(strArr[0]);
					per.setStart_date(date1.parse(strArr[1]));
					per.setEnd_date(date1.parse(strArr[2]));
					per.setActual_period(strArr[3].trim());
					c.output(per);
				}
			}
		}));

		List<String> fieldNames6 = Arrays.asList("Source_BDA", "Start_date", "End_date", "Actual_period");
		List<Integer> fieldTypes6 = Arrays.asList(Types.VARCHAR, Types.DATE, Types.DATE, Types.VARCHAR);
		final BeamRecordSqlType appType6 = BeamRecordSqlType.create(fieldNames6, fieldTypes6);

		PCollection<BeamRecord> apps6 = pojos6.apply(ParDo.of(new DoFn<ClassPeriod, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType6, c.element().Source_BDA, c.element().Start_date,
						c.element().End_date, c.element().Actual_period);
				c.output(br);
			}
		})).setCoder(appType6.getRecordCoder());
		PCollection<BeamRecord> periodM = apps6.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());


		
		// implement Spend.csv
		PCollection<String> spend_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Spend.csv"));
		PCollection<ClassSpend> pojos7 = spend_read.apply(ParDo.of(new DoFn<String, ClassSpend>() { // converting String
																									// into class typ

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().split(",", -1);
				String header = Arrays.toString(strArr);
				if (header.contains("Fiscal Year"))
					System.out.println("Header");
				else {
					ClassSpend ce = new ClassSpend();
					ce.setFiscalYear(strArr[0].trim());
					ce.setFiscalQuarter(strArr[1].trim());
					ce.setBrandChapter(strArr[2].trim());
					ce.setMarket(strArr[3].trim());
					ce.setConsumerBehavior(strArr[4].trim());
					ce.setChannel(strArr[5].trim());
					ce.setSubChannel(strArr[6].trim());
					ce.setCampaign(strArr[7].trim());
					ce.setEventName(strArr[8].trim());
					ce.setEventKey(strArr[9].trim());
					if (strArr[10].isEmpty()) {
						ce.setReportedSpend(0.00);
					} else {
						ce.setReportedSpend(Double.valueOf(strArr[10].trim()));
					}

					if (strArr[11].isEmpty()) {
						ce.setModeledSpend(0.00);
					} else {
						ce.setModeledSpend(Double.valueOf(strArr[11].trim()));
					}

					c.output(ce);
				}
			}
		}));

		List<String> fieldNames7 = Arrays.asList("FiscalYear", "FiscalQuarter", "BrandChapter", "Market",
				"ConsumerBehavior", "Channel", "SubChannel", "Campaign", "EventName", "EventKey", "ReportedSpend",
				"ModeledSpend");
		List<Integer> fieldTypes7 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE,
				Types.DOUBLE);
		final BeamRecordSqlType appType7 = BeamRecordSqlType.create(fieldNames7, fieldTypes7);

		PCollection<BeamRecord> apps7 = pojos7.apply(ParDo.of(new DoFn<ClassSpend, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType7, c.element().FiscalYear, c.element().FiscalQuarter,
						c.element().BrandChapter, c.element().Market, c.element().ConsumerBehavior, c.element().Channel,
						c.element().SubChannel, c.element().Campaign, c.element().EventName, c.element().EventKey,
						c.element().ReportedSpend, c.element().ModeledSpend);
				c.output(br);
			}
		})).setCoder(appType7.getRecordCoder());
		PCollection<BeamRecord> spend = apps7.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
		

//
//		// implement Brands_hierarchy.csv
		PCollection<String> brand_read = p
				.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/BrandHirarchy.csv"));
		PCollection<ClassBrands> pojos1 = brand_read.apply(ParDo.of(new DoFn<String, ClassBrands>() { // converting
																										// String into
																										// class typ
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (strArr.length > 8) {
					if (header.contains("Country"))
						System.out.println("Header");
					else {
						ClassBrands cbrand = new ClassBrands();
						cbrand.setCountry(strArr[0].trim());
						cbrand.setDivision(strArr[1].trim());
						cbrand.setBU(strArr[2].trim());
						cbrand.setStudio(strArr[3].trim());
						cbrand.setNeighborhoods(strArr[4].trim());
						cbrand.setBrand_Chapter(strArr[5].trim());
						cbrand.setBeneficiary(strArr[6].trim());
						cbrand.setCatlib(strArr[7].trim());
						cbrand.setProdKey(strArr[8].trim());
						c.output(cbrand);
					}
				}
			}
		}));

		List<String> fieldNames1 = Arrays.asList("Country", "Division", "BU", "Studio", "Neighborhoods",
				"Brand_Chapter", "Beneficiary", "Catlib", "ProdKey");
		List<Integer> fieldTypes1 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType1 = BeamRecordSqlType.create(fieldNames1, fieldTypes1);

		PCollection<BeamRecord> apps1 = pojos1.apply(ParDo.of(new DoFn<ClassBrands, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType1, c.element().Country, c.element().Division, c.element().BU,
						c.element().Studio, c.element().Neighborhoods, c.element().Brand_Chapter,
						c.element().Beneficiary, c.element().Catlib, c.element().ProdKey);
				c.output(br);
			}
		})).setCoder(appType1.getRecordCoder());
		PCollection<BeamRecord> brandsH = apps1.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());

//		// implement Product_Mapping.csv
		PCollection<String> pro_read = p
				.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/ProductMapping.csv"));
		PCollection<ClassProduct> pojos10 = pro_read.apply(ParDo.of(new DoFn<String, ClassProduct>() { // converting
																										// String into
																										// class type

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (header.contains("Outlet"))
					System.out.println("Header");
				else {
					ClassProduct pro = new ClassProduct();
					pro.setOutlet(strArr[0].trim());
					pro.setCatlib(strArr[1].trim());
					pro.setParent(strArr[2].trim());
					pro.setName(strArr[3].trim());
					pro.setDescription(strArr[4]);
					pro.setDisplayOrder(strArr[5]);
					pro.setProdKey(strArr[6].trim());
					pro.setProdLvl(strArr[7].trim());
					pro.setProductAbbreviation(strArr[8].trim());
					pro.setCatKey(strArr[9].trim());
					pro.setProdAttribute(strArr[10].trim());
					pro.setDepth(Double.valueOf(strArr[11].trim().replace("", "0")));
					pro.setLineage(strArr[12].trim());
					pro.setLeaf(Double.valueOf(strArr[13].trim().replace("", "0")));
					pro.setAgg109Lvl(strArr[14].trim());
					pro.setParentKey(strArr[15].trim());
					c.output(pro);
				}
			}
		}));

		List<String> fieldNames10 = Arrays.asList("Outlet", "Catlib", "Parent", "Name", "Description", "DisplayOrder",
				"ProdKey", "ProdLvl", "ProductAbbreviation", "CatKey", "ProdAttribute", "Depth", "Lineage", "Leaf",
				"Agg109Lvl", "ParentKey");
		List<Integer> fieldTypes10 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType10 = BeamRecordSqlType.create(fieldNames10, fieldTypes10);

		PCollection<BeamRecord> apps10 = pojos10.apply(ParDo.of(new DoFn<ClassProduct, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType10, c.element().Outlet, c.element().Catlib, c.element().Parent,
						c.element().Name, c.element().Description, c.element().DisplayOrder, c.element().ProdKey,
						c.element().ProdLvl, c.element().ProductAbbreviation, c.element().CatKey,
						c.element().ProdAttribute, c.element().Depth, c.element().Lineage, c.element().Leaf,
						c.element().Agg109Lvl, c.element().ParentKey);
				c.output(br);
			}
		})).setCoder(appType10.getRecordCoder());
		PCollection<BeamRecord> product = apps10.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
//		
//		// implement CompositeEvents.csv
		PCollection<String> event_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Events/EVENTS"));
		PCollection<ClassEvents> pojos3 = event_read.apply(ParDo.of(new DoFn<String, ClassEvents>() { // converting
																										// String into
																										// class
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws IOException {

				String[] strArr = c.element().split(",");
				for (int j = 0; j < strArr.length; j++) {
					String header = Arrays.toString(strArr);
					if (strArr.length > 3) {
						String[] strsplit = strArr[3].split("\\|"); // "|" char needs to be escaped in the regex of
																	// split() because | has special meaning and we want
																	// just the | char
						// create class event for each event in column_3
						for (int i = 0; i < strsplit.length; i++) {
							if (header.contains("EvntType"))
								System.out.println("Header");
							else {
								ClassEvents clr = new ClassEvents();
								clr.setEventType(strArr[0].trim());
								clr.setEventKey(strArr[1].trim());
								clr.setEventName(strArr[2].trim());
								clr.setEventComponents(strsplit[i].trim()); // increment for each event
								c.output(clr);
							}
						}
					}
				}
			}
		}));

		List<String> fieldNames3 = Arrays.asList("EventType", "EventKey", "EventName", "EventComponents");
		List<Integer> fieldTypes3 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType3 = BeamRecordSqlType.create(fieldNames3, fieldTypes3);

		PCollection<BeamRecord> apps3 = pojos3.apply(ParDo.of(new DoFn<ClassEvents, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType3, c.element().EventType, c.element().EventKey,
						c.element().EventName, c.element().EventComponents);
				c.output(br);
			}
		})).setCoder(appType3.getRecordCoder());
		PCollection<BeamRecord> Event1 = apps3.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
		
		PCollection<BeamRecord> Event = Event1.apply(BeamSql.query(
				"SELECT DISTINCT EventType, EventKey, EventName, EventComponents from PCOLLECTION"));
		

//		PCollection<Composite_events> Composite_events = Event.apply(ParDo.of(new DoFn<BeamRecord, Composite_events>() {
//			private static final long serialVersionUID = 1L;
//
//			@ProcessElement
//			public void processElement(ProcessContext c) throws ParseException {
//				BeamRecord record = c.element();
//				String strArr = record.toString();
//				String strArr1 = strArr.substring(24);
//				String xyz = strArr1.replace("]", "");
//				String[] strArr2 = xyz.split(",");
//				Composite_events moc = new Composite_events();
//				moc.setEvntType(strArr2[0]);
//				moc.setEvntKey(strArr2[1]);
//				moc.setEvntName(strArr2[2]);
//				moc.setEvntComponents(strArr2[3]);	
//				c.output(moc);
//			}
//		}));
//		TableSchema tableSchema = new TableSchema().setFields(
//				ImmutableList.of(new TableFieldSchema().setName("Event_Type").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Event_Key").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Event_Name").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Event_Components").setType("STRING").setMode("NULLABLE")
//						));
//		TableReference tableSpec = BigQueryHelpers.parseTableSpec("ad-efficiency-dev:Ad_Efficiency.Composite_Map"); // project_id:Dataset.table
//		System.out.println("Start Bigquery");
//		
//		Composite_events
//				.apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via((Composite_events elem) -> new TableRow()
//						.set("Event_Type", elem.EvntType).set("Event_Key", elem.EvntKey).set("Event_Name", elem.EvntName)
//						.set("Event_Components", elem.EvntComponents)))
//				.apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(tableSchema)
//						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//						.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));				
////		// WeeklyDuetoFile
	
	
	PCollection<String> weekly_read = p.apply(TextIO.read()
				.from("gs://ad_efficiency_input/Weekly_Dueto/Grocery*.csv"));
		PCollection<FileIO.ReadableFile> weeklyB = p
				.apply(FileIO.match().filepattern(
						"gs://ad_efficiency_input/Weekly_Dueto/Grocery*.csv")) // reading
																													// all
																													// the
																													// files
																													// with
																													// specific
																													// nomenclature
				.apply(FileIO.readMatches());
		PCollection<String> weeklyB1 = weeklyB.apply(ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void process(ProcessContext c) {
				FileIO.ReadableFile f = c.element();
				filename_WeeklyD = f.getMetadata().resourceId().toString();
				al.add(filename_WeeklyD);
			}
		}));
		// Writing input files to Archive folder
		weekly_read
				.apply(TextIO.write().to("gs://cloroxtegadeff/final_input/Weekly_dueto_Archive/Archived_weeklyDueto"));

		final PCollectionView<String> weeklyC = weeklyB1.apply(View.<String>asSingleton());

		PCollection<ClassWeeklyDueto> pojos = weekly_read.apply(ParDo.of(new DoFn<String, ClassWeeklyDueto>() { // converting
																												// String
																												// into
																												// class
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				String[] strArr = c.element().split(",", -1);
				String header = Arrays.toString(strArr);
				if (header.contains("Prodkey"))
					System.out.println("Header");
				else {
					for (int i = 0; i < al.size(); i++) {
						String namesList = al.get(i).toString();
						String[] x = namesList.split("_");
						String x1 = x[7];
						String x2 = x[4];
						ClassWeeklyDueto wd = new ClassWeeklyDueto();
						wd.setOutlet(strArr[0]);
						wd.setCatlib(strArr[1]);
						wd.setProdKey(strArr[2]);
						wd.setGeogkey(Double.valueOf(strArr[3]));
						wd.setWeek(date1.parse(strArr[4]));
						wd.setSalesComponent(strArr[5]);
						wd.setDueto_value(Double.valueOf(strArr[6]));
						wd.setPrimaryCausalKey(strArr[7]);
						if (strArr[8].isEmpty()) {
							wd.setCausal_value(0.00);
						} else {
							wd.setCausal_value(Double.valueOf(strArr[8]));
						}
						// wd.setCountry(strArr[9].trim());
						wd.setIteration(x1);
						wd.setSourceBDA(x2);
						wd.setCountry("US");
						c.output(wd);
					}
				}
			}
		}).withSideInputs(weeklyC));

		List<String> fieldNames = Arrays.asList("Outlet", "Catlib", "ProdKey", "Geogkey", "Week", "SalesComponent",
				"Dueto_value", "PrimaryCausalKey", "Causal_value", "Iteration", "SourceBDA","Country");
		List<Integer> fieldTypes = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DATE,
				Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);

		final BeamRecordSqlType appTypeX = BeamRecordSqlType.create(fieldNames, fieldTypes);

		PCollection<BeamRecord> appsX = pojos.apply(ParDo.of(new DoFn<ClassWeeklyDueto, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appTypeX, c.element().Outlet, c.element().Catlib, c.element().ProdKey,
						c.element().Geogkey, c.element().Week, c.element().SalesComponent, c.element().Dueto_value,
						c.element().PrimaryCausalKey, c.element().Causal_value, c.element().Iteration,
						c.element().SourceBDA,c.element().Country);
				c.output(br);
			}
		})).setCoder(appTypeX.getRecordCoder());
		PCollection<BeamRecord> weekD1 = appsX.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());

		
	PCollection<BeamRecord> weekD = weekD1
			.apply(BeamSql.query("SELECT DISTINCT * FROM PCOLLECTION"));

		
//		// implement EVNTEXEC.csv
		PCollection<String> eve_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/Event_Exec.csv"));
		PCollection<ClassEventExec> pojos11 = eve_read.apply(ParDo.of(new DoFn<String, ClassEventExec>() { // converting
																											// String
																											// into
																											// class typ
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);

				if (header.contains("Brand"))
					System.out.println("Header");
				else {
					ClassEventExec cex = new ClassEventExec();
					// cex.setBrand(strArr[0].trim());
					cex.setCopy(strArr[1].trim());
					cex.setStart(strArr[2].trim());
					cex.setStop(strArr[3].trim());
					cex.setGRPs(Double.valueOf(strArr[4]));
					cex.setEvntType(strArr[5].trim());
					cex.setEvntKey(strArr[6].trim());
					cex.setEvntName(strArr[7].trim());
					c.output(cex);
				}
			}
		}));
//
		List<String> fieldNames11 = Arrays.asList("Copy", "GRPs", "Start", "Stop", "EvntType", "EvntKey", "EvntName");
		List<Integer> fieldTypes11 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType11 = BeamRecordSqlType.create(fieldNames11, fieldTypes11);

		PCollection<BeamRecord> apps11 = pojos11.apply(ParDo.of(new DoFn<ClassEventExec, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType11, c.element().Copy, c.element().Start, c.element().Stop,
						c.element().GRPs, c.element().EvntType, c.element().EvntKey, c.element().EvntName);
				c.output(br);
			}
		})).setCoder(appType11.getRecordCoder());
		PCollection<BeamRecord> eventExec = apps11.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());


//		// implement Hispanicgrp.csv
		PCollection<String> hisp_read = p.apply(TextIO.read().from("gs://cloroxtegadeff/final_input/HispanicGrp.csv"));
		PCollection<ClassHispanic> pojos12 = hisp_read.apply(ParDo.of(new DoFn<String, ClassHispanic>() { // converting
																											// String
																											// into
																											// class typ
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				String[] strArr = c.element().split(",");
				String header = Arrays.toString(strArr);
				if (strArr.length > 7) {
					if (header.contains("Market"))
						System.out.println("Header");
					else {
						ClassHispanic ch = new ClassHispanic();
						ch.setMarket(strArr[0].trim());
						ch.setWeekEnding(date1.parse(strArr[1].trim()));
						ch.setBrandVariant(strArr[2].trim());
						ch.setCreative(strArr[3].trim());
						ch.setBrand(strArr[4].trim());
						ch.setCommercialDuration(strArr[5].trim());
						if(strArr[6].isEmpty()) {
							ch.setAdvertisement_id(0.00);
						}else {
							ch.setAdvertisement_id(Double.valueOf(strArr[6]));
						}
						if(strArr[7].isEmpty()) {
							ch.setTVHousehold(0.00);
						}else {
							ch.setTVHousehold(Double.valueOf(strArr[7]));
						}
						c.output(ch);
					}
				}
			}
		}));
//
		List<String> fieldNames12 = Arrays.asList("Market", "WeekEnding", "BrandVariant", "Creative", "Brand",
				"CommercialDuration", "Advertisement_id", "TVHousehold");
		List<Integer> fieldTypes12 = Arrays.asList(Types.VARCHAR, Types.DATE, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE);
		final BeamRecordSqlType appType12 = BeamRecordSqlType.create(fieldNames12, fieldTypes12);

		PCollection<BeamRecord> apps12 = pojos12.apply(ParDo.of(new DoFn<ClassHispanic, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType12, c.element().Market, c.element().WeekEnding,
						c.element().BrandVariant, c.element().Creative, c.element().Brand,
						c.element().CommercialDuration, c.element().Advertisement_id, c.element().TVHousehold);
				c.output(br);
			}
		})).setCoder(appType12.getRecordCoder());
		PCollection<BeamRecord> hispanic = apps12.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());


		
//		// Reading Efficiency table from BigQuery
		PCollection<TableRow> qData = p
				.apply(BigQueryIO.readTableRows().fromQuery("SELECT * FROM [Ad_Efficiency.Efficiency]"));

		List<String> fieldNames13 = Arrays.asList("Level", "Country", "PeriodType", "ReportPeriod", "ActualPeriod",
				"SourceBDA", "BrandChapter", "Studio", "Neighborhoods", "BU", "Division", "SubChannel", "MediaChannel",
				"Market", "ConsumerBehaviour", "EventName", "Published", "Spend", "GRP", "Duration", "Continuity",
				"Basis", "BasisDur", "Alpha", "Beta", "Typical", "Beneficiary", "Catlib", "DueToVol", "DartSpendActual",
				"PlannedSpendMediaTools", "AdstockRatio", "Channel", "ChVol", "AOVol", "Proj", "rNR", "rNCS", "rCtb",
				"rAC", "rCtbNorm", "rACNorm", "V", "CPP", "NCS", "Ctb", "AC", "NCS1", "Ctb1", "AC1", "Cont",
				"xMarginal", "vNorm", "xNorm", "xNormOld");

		List<Integer> fieldTypes13 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,

				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,

				Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE);

		final BeamRecordSqlType appType13 = BeamRecordSqlType.create(fieldNames13, fieldTypes13);
		PCollection<BeamRecord> efficiency1 = qData.apply(ParDo.of(new DoFn<TableRow, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				TableRow row = c.element();
				String Level = (String) row.get("Level");
				String Country = (String) row.get("Country");
				String PeriodType = (String) row.get("Period_Type");
				String ReportPeriod = (String) row.get("Report_Period");
				String ActualPeriod = (String) row.get("Actual_Period");
				String SourceBDA = (String) row.get("Source_BDA");
				String BrandChapter = (String) row.get("Brand_Chapter");
				String Studio = (String) row.get("Studio");
				String Neighborhoods = (String) row.get("Neighborhoods");
				String BU = (String) row.get("BU");
				String Division = (String) row.get("Division");
				String SubChannel = (String) row.get("Sub_channel");
				String MediaChannel = (String) row.get("Media_Channel");
				String Market = (String) row.get("Market");
				String ConsumerBehaviour = (String) row.get("Consumer_Behaviour");
				String EventName = (String) row.get("Event_Name");
				String Published = (String) row.get("Published");
				Double Spend = (Double) row.get("Spend");
				if(Spend == null) {
					Spend = 0.00;
				}
				Double GRP = (Double) row.get("GRP");
			    Double Duration = (Double) row.get("Duration");
				if(Duration == null) {
					Duration = Double.valueOf("0.00");
				}
				Double Continuity = (Double) row.get("Continuity");
				Double Basis = (Double) row.get("Basis");
				Double BasisDur = (Double) row.get("BasisDur");
				Double Alpha = (Double) row.get("Alpha");
				Double Beta = (Double) row.get("Beta");
				Double Typical = (Double) row.get("Typical");
				String Beneficiary = (String) row.get("Beneficiary");
				String Catlib = (String) row.get("Catlib");
				Double DueToVol = (Double) row.get("DueToVol");
				Double DartSpendActual = (Double) row.get("Dart_Spend_Actual");
				Double PlannedSpendMediaTools = (Double) row.get("Planned_Spend_Media_Tools");
				Double AdstockRatio = (Double) row.get("AdstockRatio");
				String Channel = (String) row.get("Channel");
				Double ChVol = (Double) row.get("ChVol");
				Double AOVol = (Double) row.get("AOVol");
				Double Proj = (Double) row.get("Proj");
				Double rNR = (Double) row.get("rNR");
				Double rNCS = (Double) row.get("rNCS");
				Double rCtb = (Double) row.get("rCtb");
				Double rAC = (Double) row.get("rAC");
				Double rCtbNorm = (Double) row.get("rCtbNorm");
				Double rACNorm = (Double) row.get("rACNorm");
				Double V = (Double) row.get("V");
				String CPP = (String) row.get("CPP");
				Double NCS = (Double) row.get("NCS");
				Double Ctb = (Double) row.get("Ctb");
				Double AC = (Double) row.get("AC");
				Double NCS1 = (Double) row.get("NCS1");
				Double Ctb1 = (Double) row.get("Ctb1");
				Double AC1 = (Double) row.get("AC1");
				Double Cont = (Double) row.get("Cont");
				Double xMarginal = (Double) row.get("xMarginal");
				Double vNorm = (Double) row.get("vNorm");
				Double xNorm = (Double) row.get("xNorm");
				Double xNormOld = (Double) row.get("xNormOld");

				BeamRecord br = new BeamRecord(appType13, Level, Country, PeriodType, ReportPeriod, ActualPeriod,
						SourceBDA, BrandChapter, Studio, Neighborhoods, BU, Division, SubChannel, MediaChannel, Market,
						ConsumerBehaviour, EventName, Published, Spend, GRP, Duration, Continuity, Basis, BasisDur,
						Alpha, Beta, Typical, Beneficiary, Catlib, DueToVol, DartSpendActual, PlannedSpendMediaTools,
						AdstockRatio, Channel, ChVol, AOVol, Proj, rNR, rNCS, rCtb, rAC, rCtbNorm, rACNorm, V, CPP, NCS,
						Ctb, AC, NCS1, Ctb1, AC1, Cont, xMarginal, vNorm, xNorm, xNormOld);
				c.output(br);
			}
		})).setCoder(appType13.getRecordCoder());
		PCollection<BeamRecord> efficiency = efficiency1.apply(Window.<BeamRecord>into(new GlobalWindows())
				.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
				.withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
//
//		// Queries
//		// Query_0
		
		

		PCollection<BeamRecord> rec_0 = spend.apply(BeamSql.query(
				"SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, ReportedSpend, ModeledSpend, SUBSTRING(FiscalYear FROM 3 FOR 2) as Year1, SUBSTRING(FiscalQuarter FROM 2 FOR 1) as Quarter1 from PCOLLECTION"));

		
//		// Query_1 Filter
		PCollection<BeamRecord> rec_1 = rec_0.apply(BeamSql.query(
				"SELECT *, ((cast(Year1 as double)*10) + cast(Quarter1 as double)) as Summed, 'a' as Dummy from PCOLLECTION"));
		
		
//
//		// Query_2 Range
		PCollection<BeamRecord> rec_2 = rec_1.apply(
				BeamSql.query("SELECT max(Summed) as maxed, max(Summed)-10 as least, 'a' as Dummy from PCOLLECTION"));
		
//
//		// Query_3 Latest_Spend
		PCollectionTuple query0 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_1"), rec_1)
				.and(new TupleTag<BeamRecord>("rec_2"), rec_2);
		PCollection<BeamRecord> rec_3 = query0.apply(BeamSql.queryMulti(
				"SELECT a.FiscalYear, a.FiscalQuarter, a.BrandChapter, a.Market, a.ConsumerBehavior, a.Channel, a.SubChannel, \r\n"
						+ " a.Campaign, a.EventName, a.EventKey, a.ReportedSpend, a.ModeledSpend, a.Summed, a.Dummy, b.maxed, b.least from rec_1 a INNER JOIN rec_2 b on a.Dummy = b.Dummy where a.Summed > least and a.Summed <= maxed "));

		

//		// Query_4
		PCollectionTuple query1 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_3"), rec_3)
				.and(new TupleTag<BeamRecord>("Event"), Event);
		PCollection<BeamRecord> rec_4 = query1.apply(BeamSql
				.queryMulti("SELECT a.FiscalYear,a.FiscalQuarter, a.BrandChapter,'GM' as Market,'Composite' as ConsumerBehavior, \r\n"
						+ "a.Channel, 'Composite' as SubChannel, a.Campaign,a.ReportedSpend, a.ModeledSpend,  a.Summed, a.maxed, a.least, a.Dummy, b.EventName, b.EventKey,b.EventComponents from \r\n"
						+ "rec_3 as a left join Event as b on a.EventKey = b.EventComponents where a.SubChannel <> 'Do not use' or a.EventKey <> 'Do no use' "));
		


		// Query_5 Comp Spend
		PCollection<BeamRecord> rec_5 = rec_4
				.apply(BeamSql.query("SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, \r\n"
						+ "ConsumerBehavior, Channel, SubChannel, Campaign, sum(ReportedSpend)  ReportedSpend, sum(ModeledSpend) ModeledSpend, Summed, Dummy,  maxed, least,EventName, EventKey from PCOLLECTION group by FiscalYear, \r\n"
						+ "FiscalQuarter, BrandChapter, Market, ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, maxed, least, Dummy, Summed"));
		
		

		// Query_6 Spend combined
		PCollectionTuple query3 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_3"), rec_3)
				.and(new TupleTag<BeamRecord>("rec_5"), rec_5);
		PCollection<BeamRecord> rec_6 = query3.apply(BeamSql.queryMulti(
				"SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, ReportedSpend \r\n"
						+ " , ModeledSpend, Summed, Dummy, maxed, least from rec_3 UNION ALL SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, \r\n"
						+ " ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, ReportedSpend, ModeledSpend, Summed, Dummy, maxed, least from rec_5"));

//	PCollection<BeamRecord> rec_6_test = rec_6.apply(BeamSql.query(
//				"SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, ReportedSpend, \r\n"
//						+ "ModeledSpend from PCOLLECTION"));
//
//		PCollection<ClassSpend> ClassSpend_test = rec_6_test.apply(ParDo.of(new DoFn<BeamRecord, ClassSpend>() {
//		private static final long serialVersionUID = 1L;
//
//		@ProcessElement
//		public void processElement(ProcessContext c) throws ParseException {
//			BeamRecord record = c.element();
//			String strArr = record.toString();
//			String strArr1 = strArr.substring(24);
//			String xyz = strArr1.replace("]", "");
//			String[] strArr2 = xyz.split(",");
//			ClassSpend moc = new ClassSpend();
//			moc.setFiscalYear(strArr2[0]);
//			moc.setFiscalQuarter(strArr2[1]);
//			moc.setBrandChapter(strArr2[2]);
//			moc.setMarket(strArr2[3]);
//			moc.setConsumerBehavior(strArr2[4]);
//			moc.setChannel(strArr2[5]);
//			moc.setSubChannel(strArr2[6]);
//			moc.setCampaign(strArr2[7]);
//			moc.setEventName(strArr2[8]);
//			moc.setEventKey(strArr2[9]);
//			moc.setReportedSpend(Double.parseDouble(strArr2[10]));
//			moc.setModeledSpend(Double.parseDouble(strArr2[11]));	
//			c.output(moc);
//		}
//	}));
//	TableSchema tableSchema = new TableSchema().setFields(
//			ImmutableList.of(new TableFieldSchema().setName("FiscalYear").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("FiscalQuarter").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("BrandChapter").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Market").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("ConsumerBehavior").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Channel").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("SubChannel").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Campaign").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("EventName").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("EventKey").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("ReportedSpend").setType("FLOAT64").setMode("NULLABLE"),
//					new TableFieldSchema().setName("ModeledSpend").setType("FLOAT64").setMode("NULLABLE")
//					));
//	TableReference tableSpec = BigQueryHelpers.parseTableSpec("ad-efficiency-dev:Ad_Efficiency.Spend_Combined_Beam"); // project_id:Dataset.table
//	System.out.println("Start Bigquery");
//	
//	ClassSpend_test
//			.apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via((ClassSpend elem) -> new TableRow()
//					.set("FiscalYear", elem.FiscalYear).set("FiscalQuarter", elem.FiscalQuarter).set("BrandChapter", elem.BrandChapter)
//					.set("Market", elem.Market).set("ConsumerBehavior", elem.ConsumerBehavior).set("Channel", elem.Channel).set("SubChannel", elem.SubChannel)
//					.set("Campaign", elem.Campaign).set("EventName", elem.EventName).set("EventKey", elem.EventKey).set("ReportedSpend", elem.ReportedSpend)
//					.set("ModeledSpend", elem.ModeledSpend)))
//			.apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(tableSchema)
//					.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//					.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
		
		// Query_8
		PCollection<BeamRecord> rec_8 = brandsH
				.apply(BeamSql.query("SELECT DISTINCT Catlib, ProdKey from PCOLLECTION where ProdKey <> 'null'"));

		
		// Query_9 Product_selected
		PCollectionTuple query4 = PCollectionTuple.of(new TupleTag<BeamRecord>("weekD"), weekD)
				.and(new TupleTag<BeamRecord>("rec_8"), rec_8);
		PCollection<BeamRecord> rec_9 = query4.apply(BeamSql.queryMulti(
				"SELECT a.Outlet, a.Catlib, a.SourceBDA, a.ProdKey, a.Geogkey, a.Week, a.SalesComponent, a.Dueto_value, a.PrimaryCausalKey, (case when a.Causal_value is null then cast('0.00' as double) else a.Causal_value end) as  Causal_value  ,a.Country, a.Iteration ,b.ProdKey from weekD as a \r\n"
						+ "INNER JOIN rec_8 as b on a.Catlib = b.Catlib and a.ProdKey = b.ProdKey"));
		
		
		// Query_10 BrandMap
	PCollectionTuple query5 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_9"), rec_9)
				.and(new TupleTag<BeamRecord>("brandsH"), brandsH);
		PCollection<BeamRecord> rec_10 = query5.apply(BeamSql.queryMulti(
				"Select a.Outlet, a.Catlib, a.SourceBDA, a.ProdKey, a.Week, a.SalesComponent, a.Dueto_value, a.PrimaryCausalKey,a.Iteration, a.Causal_value ,a.Country, \r\n"
						+ "b.Beneficiary from rec_9 as a LEFT JOIN brandsH as b \r\n"
						+ "on a.Catlib = b.Catlib and a.ProdKey = b.ProdKey"));

		
//		// Query_11 52_week
		PCollectionTuple query6 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_10"), rec_10)
				.and(new TupleTag<BeamRecord>("periodM"), periodM);
		PCollection<BeamRecord> rec_11 = query6.apply(BeamSql
				.queryMulti("Select a.Outlet, a.Catlib, a.SourceBDA, a.ProdKey, a.Week, b.Start_date, b.End_date, \r\n"
						+ "a.SalesComponent, a.Dueto_value, a.PrimaryCausalKey,a.Iteration, a.Causal_value ,a.Country, a.Beneficiary, b.Actual_period from rec_10 as a INNER JOIN \r\n"
						+ "periodM as b on a.SourceBDA = b.Source_BDA WHERE cast(cast(EXTRACT(YEAR from a.Week) as VARCHAR) || (case when EXTRACT(MONTH from a.Week) < 10 then '0' || cast(EXTRACT(MONTH from a.Week) as VARCHAR) else cast(EXTRACT(MONTH from a.Week) as VARCHAR) END) || (case when EXTRACT(DAY from a.Week) < 10 then '0' || cast(EXTRACT(DAY from a.Week) as VARCHAR) else  cast(EXTRACT(DAY from a.Week) as VARCHAR) END) as BIGINT) >= cast(cast(EXTRACT(YEAR from b.Start_date) as VARCHAR) || (case when EXTRACT(MONTH from b.Start_date) < 10 then '0' || cast(EXTRACT(MONTH from b.Start_date) as VARCHAR) else cast(EXTRACT(MONTH from b.Start_date) as VARCHAR) END) || (case when EXTRACT(DAY from b.Start_date) < 10 then '0' || cast(EXTRACT(DAY from b.Start_date) as VARCHAR) else  cast(EXTRACT(DAY from b.Start_date) as VARCHAR) END) as BIGINT) \r\n"
						+ "and cast(cast(EXTRACT(YEAR from a.Week) as VARCHAR) || (case when EXTRACT(MONTH from a.Week) < 10 then '0' || cast(EXTRACT(MONTH from a.Week) as VARCHAR) else cast(EXTRACT(MONTH from a.Week) as VARCHAR) END) || (case when EXTRACT(DAY from a.Week) < 10 then '0' || cast(EXTRACT(DAY from a.Week) as VARCHAR) else  cast(EXTRACT(DAY from a.Week) as VARCHAR) END) as BIGINT) <= cast(cast(EXTRACT(YEAR from b.End_date) as VARCHAR) || (case when EXTRACT(MONTH from b.End_date) < 10 then '0' || cast(EXTRACT(MONTH from b.End_date) as VARCHAR) else cast(EXTRACT(MONTH from b.End_date) as VARCHAR) END) || (case when EXTRACT(DAY from b.End_date) < 10 then '0' || cast(EXTRACT(DAY from b.End_date) as VARCHAR) else  cast(EXTRACT(DAY from b.End_date) as VARCHAR) END) as BIGINT)"));

		// Query_12
	PCollection<BeamRecord> rec_12 = rec_6.apply(BeamSql.query(
				"SELECT DISTINCT BrandChapter, SubChannel, EventName,Market,EventKey, Channel, ConsumerBehavior from PCOLLECTION"));

				
		// Query_13 Media_Map
		PCollectionTuple query7 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_11"), rec_11)
				.and(new TupleTag<BeamRecord>("rec_12"), rec_12);
		PCollection<BeamRecord> rec_13 = query7.apply(BeamSql.queryMulti(
				"Select a.Outlet, a.Catlib, a.SourceBDA, a.ProdKey, a.Week, a.SalesComponent, a.Dueto_value, a.PrimaryCausalKey \r\n"
						+ ",a.Iteration, a.Causal_value ,a.Country, a.Beneficiary, a.Actual_period,b.BrandChapter, b.SubChannel, b.EventName, b.Market,b.Channel as MediaChannel ,b.ConsumerBehavior from rec_11 as a INNER JOIN rec_12 as b \r\n"
						+ "on a.SalesComponent = b.EventKey "));
		//print--------

		
		// HISPANIC queries starts
		PCollection<BeamRecord> hispanic_temp = hispanic.apply(BeamSql.query(
				"SELECT Market, WeekEnding, BrandVariant, Creative, Brand, CommercialDuration, Advertisement_id, TVHousehold, 'H:' as HTemp, Creative as Creative_new from PCOLLECTION"));

		
		/// Hispanic_2
		PCollection<BeamRecord> hispanic_2 = hispanic_temp.apply(BeamSql.query(
				"SELECT Market, WeekEnding, BrandVariant, Creative, Brand, CommercialDuration, Advertisement_id, TVHousehold, HTemp, (HTemp||Creative) as Creative_new from PCOLLECTION"));

		// Hispanic_3
		PCollection<BeamRecord> hispanic_3 = eventExec
				.apply(BeamSql.query("Select UPPER(Copy) as Copy, GRPs, EvntType, EvntKey, EvntName from PCOLLECTION"));
		
		// Hispanic_4
		PCollectionTuple query_hispanic_4 = PCollectionTuple.of(new TupleTag<BeamRecord>("hispanic_2"), hispanic_2)
				.and(new TupleTag<BeamRecord>("hispanic_3"), hispanic_3);
		PCollection<BeamRecord> hispanic_4 = query_hispanic_4.apply(BeamSql.queryMulti(
				"Select a.Market, a.WeekEnding, a.BrandVariant, a.Creative, a.CommercialDuration, a.Advertisement_id, a.TVHousehold, a.Creative_new, b.EvntKey from hispanic_2 as a \r\n"
						+ "left join hispanic_3 as b \r\n"
						+ "ON a.Creative_new = b.Copy WHERE EvntName <> 'Do Not Use' "));

	
		PCollectionTuple query_hispanic_4_comp = PCollectionTuple.of(new TupleTag<BeamRecord>("hispanic_4"), hispanic_4)
				.and(new TupleTag<BeamRecord>("Event"), Event);
		PCollection<BeamRecord> hispanic_4_comp = query_hispanic_4_comp.apply(BeamSql.queryMulti(
				"Select a.Market, a.WeekEnding, a.BrandVariant, a.Creative, a.CommercialDuration, a.Advertisement_id, a.TVHousehold, 'Composite' as Creative_new, b.EventKey as EvntKey from hispanic_4 as a \r\n"
						+ "inner join Event as b \r\n"
						+ "ON a.EvntKey = b.EventComponents"));
		
		PCollectionTuple query_hispanic_4_union = PCollectionTuple.of(new TupleTag<BeamRecord>("hispanic_4_comp"), hispanic_4_comp)
				.and(new TupleTag<BeamRecord>("hispanic_4"), hispanic_4);
		PCollection<BeamRecord> hispanic_4_union = query_hispanic_4_union.apply(BeamSql.queryMulti(
				"SELECT * FROM hispanic_4_comp \r\n"
				+ "UNION ALL \r\n"
				+ "SELECT * FROM hispanic_4"));
		
		
		// Hispanic_5
		PCollectionTuple query_hispanic_5 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_13"), rec_13)
				.and(new TupleTag<BeamRecord>("hispanic_4_union"), hispanic_4_union);
		PCollection<BeamRecord> hispanic_5 = query_hispanic_5.apply(BeamSql.queryMulti(
				"Select a.Outlet, a.Catlib, a.SourceBDA, a.ProdKey, a.Week,a.SalesComponent, a.Dueto_value, a.PrimaryCausalKey, \r\n"
						+ "a.Iteration, a.Causal_value ,a.Country, a.Beneficiary, a.Actual_period, a.BrandChapter, a.SubChannel, a.EventName, a.Market,a.MediaChannel ,a.ConsumerBehavior, b.EvntKey, b.TVHousehold from rec_13 as a left join hispanic_4_union as b \r\n"
						+ "on a.Week = b.WeekEnding and a.SalesComponent = b.EvntKey"));
		
		// Hispanic_6
		PCollection<BeamRecord> hispanic_6 = hispanic_5.apply(BeamSql.query(
				"Select Outlet, Catlib, SourceBDA, ProdKey, Week, SalesComponent, Dueto_value, PrimaryCausalKey, \r\n"
						+ "Iteration ,Country, Beneficiary, Actual_period, BrandChapter, SubChannel, EventName, Market, MediaChannel ,ConsumerBehavior, EvntKey, TVHousehold, hispanicUDF(EvntKey,Causal_value,TVHousehold) as Causal_Value_New from PCOLLECTION").withUdf("hispanicUDF", hispanicUDF.class));
		
//		PCollection<BeamRecord> hispanic_6_temp = hispanic_6.apply(BeamSql.query(
//				"Select Outlet, Catlib, SourceBDA, ProdKey, Week, SalesComponent, Dueto_value, PrimaryCausalKey, \r\n"
//						+ "Iteration ,Country, Beneficiary, Actual_period, BrandChapter, SubChannel, EventName, Market, MediaChannel ,ConsumerBehavior, EvntKey, TVHousehold,Causal_Value_New from PCOLLECTION"));
//
//		PCollection<Hispanic6temp> Hispanic6temp = hispanic_6_temp
//				.apply(ParDo.of(new DoFn<BeamRecord, Hispanic6temp>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) throws ParseException {
//						BeamRecord record = c.element();
//						String strArr = record.toString();
//						String strArr1 = strArr.substring(24);
//						String xyz = strArr1.replace("]", "");
//						String[] strArr2 = xyz.split(",");
//						Hispanic6temp moc = new Hispanic6temp();
//						moc.setOutlet(strArr2[0]);
//						moc.setCatlib(strArr2[1]);
//						moc.setSourceBDA(strArr2[2]);
//						moc.setProdKey(strArr2[3]);
//						moc.setWeek(strArr2[4]);
//						moc.setSalesComponent(strArr2[5]);
//						moc.setDueto_value(Double.parseDouble(strArr2[6]));
//						moc.setPrimaryCausalKey(strArr2[7]);
//						moc.setIteration(strArr2[8]);
//						moc.setCountry(strArr2[9]);
//						moc.setBeneficiary(strArr2[10]);
//						moc.setActual_period(strArr2[11]);
//						moc.setBrandChapter(strArr2[12]);
//						moc.setSubChannel(strArr2[13]);
//						moc.setEventName(strArr2[14]);
//						moc.setMarket(strArr2[15]);
//						moc.setMediaChannel(strArr2[16]);
//						moc.setConsumerBehavior(strArr2[17]);
//						moc.setEvntKey(strArr2[18]);
//					    moc.setTVHousehold(strArr2[19]);
//						moc.setCausal_Value_New(Double.parseDouble(strArr2[20]));
//						c.output(moc);
//					}
//				}));
//		TableSchema tableSchema = new TableSchema().setFields(
//				ImmutableList.of(new TableFieldSchema().setName("Outlet").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Catlib").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("SourceBDA").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("ProdKey").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Week").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("SalesComponent").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Dueto_value").setType("FLOAT64").setMode("NULLABLE"),
//						new TableFieldSchema().setName("PrimaryCausalKey").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Iteration").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Country").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Beneficiary").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Actual_period").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("BrandChapter").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("SubChannel").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("EventName").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Market").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("MediaChannel").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("ConsumerBehavior").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("EvntKey").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("TVHousehold").setType("STRING").setMode("NULLABLE"),
//						new TableFieldSchema().setName("Causal_Value_New").setType("FLOAT64").setMode("NULLABLE")));
//		
//		TableReference tableSpec = BigQueryHelpers.parseTableSpec("ad-efficiency-dev:Ad_Efficiency.Hispanic6temp"); // project_id:Dataset.table
//		System.out.println("Start Bigquery");
//
//		Hispanic6temp.apply(MapElements.into(TypeDescriptor.of(TableRow.class))
//				.via((Hispanic6temp elem) -> new TableRow().set("Outlet", elem.Outlet).set("Catlib", elem.Catlib)
//						.set("SourceBDA", elem.SourceBDA).set("ProdKey", elem.ProdKey).set("Week", elem.Week)
//						.set("SalesComponent", elem.SalesComponent).set("Dueto_value", elem.Dueto_value)
//						.set("PrimaryCausalKey", elem.PrimaryCausalKey).set("Iteration", elem.Iteration)
//						.set("Country", elem.Country).set("Beneficiary", elem.Beneficiary)
//						.set("Actual_period", elem.Actual_period).set("BrandChapter", elem.BrandChapter)
//						.set("SubChannel", elem.SubChannel).set("EventName", elem.EventName).set("Market", elem.Market)
//						.set("MediaChannel", elem.MediaChannel).set("ConsumerBehavior", elem.ConsumerBehavior)
//						.set("EvntKey", elem.EvntKey).set("TVHousehold", elem.TVHousehold)
//						.set("Causal_Value_New", elem.Causal_Value_New)))
//				.apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(tableSchema)
//						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//						.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
		
		
		
//		// Duration Query calculation part
   PCollection<BeamRecord> X = Wknd.apply(BeamSql.query(
				"SELECT EveDate,1 as Dummy, cast(cast(EXTRACT(YEAR from EveDate) as VARCHAR) || (case when EXTRACT(MONTH from EveDate) < 10 then '0' || cast(EXTRACT(MONTH from EveDate) as VARCHAR) else cast(EXTRACT(MONTH from EveDate) as VARCHAR) END) || (case when EXTRACT(DAY from EveDate) < 10 then '0' || cast(EXTRACT(DAY from EveDate) as VARCHAR) else cast(EXTRACT(DAY from EveDate) as VARCHAR) END) as BIGINT) as Weekend from PCOLLECTION"));


		// Query_2 Range
		PCollection<BeamRecord> temp = hispanic_6.apply(BeamSql.query(
				"SELECT Outlet, Week, SalesComponent as eventid, cast(cast(EXTRACT(YEAR from Week) as VARCHAR) || (case when EXTRACT(MONTH from Week) < 10 then '0' || cast(EXTRACT(MONTH from Week) as VARCHAR) else cast(EXTRACT(MONTH from Week) as VARCHAR) END) || (case when EXTRACT(DAY from Week) < 10 then '0' || cast(EXTRACT(DAY from Week) as VARCHAR) else  cast(EXTRACT(DAY from Week) as VARCHAR) END) as BIGINT) as Week1, (CASE when MediaChannel='Digital' then 1 else 5 end) Limit1, AVG(Causal_Value_New) as grp from PCOLLECTION \r\n"
						+ " WHERE cast(cast(EXTRACT(YEAR from Week) as VARCHAR) || (case when EXTRACT(MONTH from Week) < 10 then '0' || cast(EXTRACT(MONTH from Week) as VARCHAR) else cast(EXTRACT(MONTH from Week) as VARCHAR) END) || (case when EXTRACT(DAY from Week) < 10 then '0' || cast(EXTRACT(DAY from Week) as VARCHAR) else  cast(EXTRACT(DAY from Week) as VARCHAR) END) as BIGINT) >= 20161225 and \r\n"
						+ " cast(cast(EXTRACT(YEAR from Week) as VARCHAR) || (case when EXTRACT(MONTH from Week) < 10 then '0' || cast(EXTRACT(MONTH from Week) as VARCHAR) else cast(EXTRACT(MONTH from Week) as VARCHAR) END) || (case when EXTRACT(DAY from Week) < 10 then '0' || cast(EXTRACT(DAY from Week) as VARCHAR) else  cast(EXTRACT(DAY from Week) as VARCHAR) END) as BIGINT) <= 20171217 GROUP BY Outlet, SalesComponent, Week, MediaChannel"));
		
		PCollection<BeamRecord> events = temp.apply(BeamSql
				.query("SELECT Outlet, Limit1, eventid, Week from PCOLLECTION group by eventid, Week,Limit1,Outlet"));

		// Events
		PCollection<BeamRecord> events2 = events.apply(BeamSql.query(
				"SELECT Outlet, Limit1, eventid,1 as Dummy, MIN(cast(cast(EXTRACT(YEAR from Week) as VARCHAR) || (case when EXTRACT(MONTH from Week) < 10 then '0' || cast(EXTRACT(MONTH from Week) as VARCHAR) else cast(EXTRACT(MONTH from Week) as VARCHAR) END) || (case when EXTRACT(DAY from Week) < 10 then '0' || cast(EXTRACT(DAY from Week) as VARCHAR) else  cast(EXTRACT(DAY from Week) as VARCHAR) END) as BIGINT)) as mindate, \r\n"
						+ " MAX(cast(cast(EXTRACT(YEAR from Week) as VARCHAR) || (case when EXTRACT(MONTH from Week) < 10 then '0' || cast(EXTRACT(MONTH from Week) as VARCHAR) else cast(EXTRACT(MONTH from Week) as VARCHAR) END) || (case when EXTRACT(DAY from Week) < 10 then '0' || cast(EXTRACT(DAY from Week) as VARCHAR) else  cast(EXTRACT(DAY from Week) as VARCHAR) END) as BIGINT)) as maxdate from PCOLLECTION group by Outlet, eventid, Limit1"));


		
	    PCollectionTuple queryx = PCollectionTuple.of(new TupleTag<BeamRecord>("events2"), events2)
				.and(new TupleTag<BeamRecord>("X"), X);
		PCollection<BeamRecord> seq = queryx.apply(BeamSql.queryMulti(
				"SELECT a.Outlet,a.eventid, a.Limit1, a.Dummy, a.mindate, a.maxdate, b.Weekend, b.EveDate from events2 a RIGHT JOIN X b ON a.Dummy = b.Dummy"));

		
		// SequenceEvents
		PCollection<BeamRecord> seqevents = seq.apply(BeamSql.query(
				"SELECT Outlet, eventid, Limit1,Dummy, mindate, maxdate, Weekend, EveDate, cast(cast(EXTRACT(YEAR from EveDate) as VARCHAR) || (case when EXTRACT(MONTH from EveDate) < 10 then '0' || cast(EXTRACT(MONTH from EveDate) as VARCHAR) else cast(EXTRACT(MONTH from EveDate) as VARCHAR) END) || (case when EXTRACT(DAY from EveDate) < 10 then '0' || cast(EXTRACT(DAY from EveDate) as VARCHAR) else cast(EXTRACT(DAY from EveDate) as VARCHAR) END) as BIGINT) as EveDate1 from PCOLLECTION where Weekend >= mindate and Weekend <= maxdate"));
		
		// temp2
		PCollectionTuple queryy = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
				.and(new TupleTag<BeamRecord>("temp"), temp);
		PCollection<BeamRecord> temp2 = queryy.apply(BeamSql.queryMulti(
				"select a.Outlet,a.EveDate1 as Weekint,a.Limit1,a.eventid as eventidA, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, \r\n"
				+ "b.grp from seqevents a left join temp b ON (a.EveDate1 = b.Week1) and (a.eventid = b.eventid) where a.eventid is not null"));

		
		PCollectionTuple queryy1 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
				.and(new TupleTag<BeamRecord>("temp"), temp);
		PCollection<BeamRecord> temp3 = queryy1.apply(BeamSql.queryMulti(
				"select a.Outlet,a.EveDate1 as Weekint,a.Limit1,a.eventid as eventidA, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, \r\n"
				+ "b.grp from seqevents a left join temp b ON (a.EveDate1 = b.Week1) and (a.eventid = b.eventid) where a.eventid is not null"));
		
		PCollectionTuple queryy2 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
				.and(new TupleTag<BeamRecord>("temp"), temp);
		PCollection<BeamRecord> temp4 = queryy2.apply(BeamSql.queryMulti(
				"select a.Outlet,a.EveDate1 as Weekint,a.Limit1,a.eventid as eventidA, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, \r\n"
				+ "b.grp from seqevents a left join temp b ON (a.EveDate1 = b.Week1) and (a.eventid = b.eventid) where a.eventid is not null"));
		
		PCollectionTuple queryy3 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
				.and(new TupleTag<BeamRecord>("temp"), temp);
		PCollection<BeamRecord> temp5 = queryy3.apply(BeamSql.queryMulti(
				"select a.Outlet,a.EveDate1 as Weekint,a.Limit1,a.eventid as eventidA, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, \r\n"
				+ "b.grp from seqevents a left join temp b ON (a.EveDate1 = b.Week1) and (a.eventid = b.eventid) where a.eventid is not null"));
		
		PCollectionTuple queryy4 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
				.and(new TupleTag<BeamRecord>("temp"), temp);
		PCollection<BeamRecord> temp6 = queryy4.apply(BeamSql.queryMulti(
				"select a.Outlet,a.EveDate1 as Weekint,a.Limit1,a.eventid as eventidA, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, \r\n"
				+ "b.grp from seqevents a left join temp b ON (a.EveDate1 = b.Week1) and (a.eventid = b.eventid) where a.eventid is not null"));
		
		// cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) as Weekint,

		
		//PCollection<BeamRecord> temp2 = tempX
				//.apply(BeamSql.query("SELECT * from PCOLLECTION where eventidA = eventidB"));

				
//		// temp3
//		PCollectionTuple queryy1 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
//				.and(new TupleTag<BeamRecord>("temp"), temp);
//		PCollection<BeamRecord> tempX1 = queryy1.apply(BeamSql.queryMulti(
//				"select a.Outlet, a.EveDate1,a.Limit1, a.eventid as eventidA,b.eventid as eventidB, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, b.Week, b.Week1, cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) as Weekint, b.grp from seqevents a left join temp b ON a.EveDate1 = b.Week1 where a.eventid is not null"));
//		PCollection<BeamRecord> temp3 = tempX1
//				.apply(BeamSql.query("SELECT * from PCOLLECTION where eventidA = eventidB"));
//
//		// temp4
//		PCollectionTuple queryy2 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
//				.and(new TupleTag<BeamRecord>("temp"), temp);
//		PCollection<BeamRecord> tempX2 = queryy2.apply(BeamSql.queryMulti(
//				"select a.Outlet, a.EveDate1,a.Limit1, a.eventid as eventidA,b.eventid as eventidB, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, b.Week, b.Week1, cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) as Weekint, b.grp from seqevents a left join temp b ON a.EveDate1 = b.Week1 where a.eventid is not null"));
//		PCollection<BeamRecord> temp4 = tempX2
//				.apply(BeamSql.query("SELECT * from PCOLLECTION where eventidA = eventidB"));
//
//		
//		// temp5
//		PCollectionTuple queryy3 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
//				.and(new TupleTag<BeamRecord>("temp"), temp);
//		PCollection<BeamRecord> tempX3 = queryy3.apply(BeamSql.queryMulti(
//				"select a.Outlet, a.EveDate1, a.Limit1, a.eventid as eventidA, b.eventid as eventidB, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, b.Week, b.Week1, cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) as Weekint, b.grp from seqevents a left join temp b ON a.EveDate1 = b.Week1 where a.eventid is not null"));
//		PCollection<BeamRecord> temp5 = tempX3
//				.apply(BeamSql.query("SELECT * from PCOLLECTION where eventidA = eventidB"));
//
//
//		
//		// temp6
//		PCollectionTuple queryy4 = PCollectionTuple.of(new TupleTag<BeamRecord>("seqevents"), seqevents)
//				.and(new TupleTag<BeamRecord>("temp"), temp);
//		PCollection<BeamRecord> tempX4 = queryy4.apply(BeamSql.queryMulti(
//				"select a.Outlet, a.EveDate1,a.Limit1, a.eventid as eventidA, b.eventid as eventidB, a.Dummy, a.mindate, a.maxdate, a.Weekend, a.EveDate, b.Week, b.Week1, cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) as Weekint, b.grp from seqevents a left join temp b ON a.EveDate1 = b.Week1 where a.eventid is not null"));
//		PCollection<BeamRecord> temp6 = tempX4
//				.apply(BeamSql.query("SELECT * from PCOLLECTION where eventidA = eventidB"));


		// Query_3 Latest_Spend
		
		PCollectionTuple querySpend = PCollectionTuple.of(new TupleTag<BeamRecord>("temp2"), temp2)
				.and(new TupleTag<BeamRecord>("temp3"), temp3).and(new TupleTag<BeamRecord>("temp4"), temp4)
				.and(new TupleTag<BeamRecord>("temp5"), temp5).and(new TupleTag<BeamRecord>("temp6"), temp6);
		PCollection<BeamRecord> Duration_Flag_test = querySpend.apply(BeamSql.queryMulti("SELECT a.*, \r\n"
				+ "(case \r\n"
				+ "when a.grp >= a.Limit1 then 1 \r\n" + "when b.grp >= a.Limit1 then 1 \r\n"
				+ "when c.grp >= a.Limit1 then 1 \r\n" + "when d.grp >= a.Limit1 then 1 \r\n"
				+ "when e.grp >= a.Limit1 then 1 \r\n" + "else 0 end) as flag,(CASE WHEN a.grp >= CAST('20.00' AS DOUBLE) then CAST('1.00' AS DOUBLE) else CAST('0.00' AS DOUBLE) end) as contiflag \r\n" 
				+ "from temp2 a left join \r\n"
				+ "temp3 b on a.eventidA = b.eventidA and b.Weekint = cast(sqlUDF1(cast(a.Weekint as VARCHAR),7) as BIGINT) left join \r\n"
				+ "temp4 c on a.eventidA = c.eventidA and c.Weekint = cast(sqlUDF1(cast(a.Weekint as VARCHAR),14) as BIGINT) left join \r\n"
				+ "temp5 d on a.eventidA = d.eventidA and d.Weekint = cast(sqlUDF1(cast(a.Weekint as VARCHAR),21) as BIGINT) left join \r\n"
				+ "temp6 e on a.eventidA = e.eventidA and e.Weekint = cast(sqlUDF1(cast(a.Weekint as VARCHAR),28) as BIGINT)")
				.withUdf("sqlUDF1", AddS.class));


		PCollection<BeamRecord> Duration_Flag = Duration_Flag_test.apply(BeamSql.query("select Outlet,Weekint,Limit1,eventidA,Dummy,mindate, \r\n"
				+ "maxdate,Weekend,EveDate,duration_flag_UDF(grp) as grp,flag,contiflag FROM PCOLLECTION ").withUdf("duration_flag_UDF", duration_flag_UDF.class));

//		PCollection<Durationflag> Durationflag = Duration_Flag.apply(ParDo.of(new DoFn<BeamRecord, Durationflag>() {
//		private static final long serialVersionUID = 1L;
//
//		@ProcessElement
//		public void processElement(ProcessContext c) throws ParseException {
//			BeamRecord record = c.element();
//			String strArr = record.toString();
//			String strArr1 = strArr.substring(24);
//			String xyz = strArr1.replace("]", "");
//			String[] strArr2 = xyz.split(",");
//			Durationflag moc = new Durationflag();
//			moc.setOutlet(strArr2[0]);
//			moc.setWeekint(strArr2[1]);
//			moc.setLimit1(strArr2[2]);
//			moc.setEventidA(strArr2[3]);
//			moc.setDummy(strArr2[4]);
//			moc.setMindate(strArr2[5]);
//			moc.setMaxdate(strArr2[6]);
//			moc.setWeekend(strArr2[7]);
//			moc.setEveDate(strArr2[8]);
//			moc.setGrp(Double.parseDouble(strArr2[9]));
//			moc.setFlag(Double.parseDouble(strArr2[10]));
//      		c.output(moc);
//		}
//	}));
//	TableSchema tableSchema = new TableSchema().setFields(
//			ImmutableList.of(new TableFieldSchema().setName("Outlet").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Weekint").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Limit1").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("eventidA").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Dummy").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("mindate").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("maxdate").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("Weekend").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("EveDate").setType("STRING").setMode("NULLABLE"),
//					new TableFieldSchema().setName("grp").setType("FLOAT64").setMode("NULLABLE"),
//					new TableFieldSchema().setName("flag").setType("FLOAT64").setMode("NULLABLE")
//					));
//	TableReference tableSpec = BigQueryHelpers.parseTableSpec("ad-efficiency-dev:Ad_Efficiency.Durationflag"); // project_id:Dataset.table
//	System.out.println("Start Bigquery");
//	
//	Durationflag
//			.apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via((Durationflag elem) -> new TableRow()
//					.set("Outlet", elem.Outlet).set("Weekint", elem.Weekint).set("Limit1", elem.Limit1)
//					.set("eventidA", elem.eventidA)
//					.set("Dummy", elem.Dummy).set("mindate", elem.mindate).set("maxdate", elem.maxdate)
//					.set("Weekend", elem.Weekend)
//					.set("EveDate", elem.EveDate).set("grp", elem.grp).set("flag", elem.flag)
//					))
//			.apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(tableSchema)
//					.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//					.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
		
//		// Query_14 Execution_start
		PCollection<BeamRecord> rec_14 = rec_13.apply(BeamSql.query(
				"SELECT SourceBDA,Country,Market, SubChannel, EventName, SalesComponent as Event , Catlib, Beneficiary, \r\n"
						+ "Outlet as Channel , Actual_period as Actual_Period, BrandChapter, MediaChannel ,ConsumerBehavior from PCOLLECTION"
						+ " group by SubChannel, EventName, Market, SalesComponent,\r\n"
						+ " Catlib , Beneficiary, Outlet, Actual_period, BrandChapter ,MediaChannel ,ConsumerBehavior, SourceBDA,Country"));

		// Query_15
		PCollection<BeamRecord> rec_15 = periodM
				.apply(BeamSql.query("SELECT Start_date, End_date, Actual_period from PCOLLECTION "));

		// Query_16 Min_Max_Date
		PCollectionTuple query8 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_14"), rec_14)
				.and(new TupleTag<BeamRecord>("rec_15"), rec_15);
		PCollection<BeamRecord> rec_16 = query8.apply(BeamSql.queryMulti(
				"SELECT a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event , a.Catlib, a.Beneficiary, \r\n"
						+ "a.Channel , a.Actual_Period, a.BrandChapter, a.MediaChannel ,a.ConsumerBehavior,b.Start_date as PeriodStartDate, b.End_date as PeriodEndDate \r\n"
						+ " from rec_14 as a left join rec_15 as b on a.Actual_Period = b.Actual_period "));

		// Query_17
		PCollectionTuple query9 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_16"), rec_16)
				.and(new TupleTag<BeamRecord>("hispanic_6"), hispanic_6);
		PCollection<BeamRecord> rec_17temp = query9.apply(BeamSql.queryMulti(
				"select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event, a.Catlib, a.Beneficiary, \r\n"
						+ "		 a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.BrandChapter, a.MediaChannel, a.ConsumerBehavior, SUM(b.Causal_Value_New) as Causal_Value_New from rec_16 a \r\n"
						+ "        left join hispanic_6 b \r\n"
						+ "        on a.Channel = b.Outlet and a.Event = b.SalesComponent and a.Beneficiary = b.Beneficiary \r\n"
						+ "		 WHERE cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) >= cast(cast(EXTRACT(YEAR from a.PeriodStartDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) END) as BIGINT) and \r\n"
						+ "	     cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) <= cast(cast(EXTRACT(YEAR from a.PeriodEndDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) END) as BIGINT) \r\n"
						+ " 		 group by a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event, a.Catlib, a.Beneficiary, \r\n"
	                    + "		 Channel, a.Actual_Period, PeriodStartDate, PeriodEndDate, a.BrandChapter, a.MediaChannel, a.ConsumerBehavior"));
			
		// Execution_grp
		PCollection<BeamRecord> rec_17 = rec_17temp.apply(
				BeamSql.query("SELECT SourceBDA,Country,Market, SubChannel, EventName, Event, Catlib, Beneficiary, \r\n"
						+ "		Channel, Actual_Period, PeriodStartDate, PeriodEndDate, BrandChapter, MediaChannel, ConsumerBehavior,Causal_Value_New as GRPs from PCOLLECTION "));


		
		// Query_18 Execution_Copy_Start
		PCollectionTuple query10 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_17"), rec_17)
				.and(new TupleTag<BeamRecord>("hispanic_6"), hispanic_6);
		PCollection<BeamRecord> rec_18 = query10.apply(BeamSql.queryMulti(
				"select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event, a.Catlib, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "a.PeriodStartDate, a.BrandChapter, a.PeriodEndDate, a.MediaChannel ,a.ConsumerBehavior,MIN(cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT)) as CopyStartWithinPeriod, MAX(cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT)) as CopyEndWithinPeriod, a.GRPs \r\n"
						+ "from rec_17 a left join hispanic_6 b \r\n" + "on a.Channel = b.Outlet and \r\n"
						+ "a.Beneficiary = b.Beneficiary and \r\n" + "a.Event = b.SalesComponent \r\n"
						//+ "WHERE cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) >= cast(cast(EXTRACT(YEAR from a.PeriodStartDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) END) as BIGINT) and \r\n"
						//+ "cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) <= cast(cast(EXTRACT(YEAR from a.PeriodEndDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) END) as BIGINT) and \r\n"
						+ "WHERE b.Causal_Value_New > 0.0 \r\n"
						+ "group by a.SourceBDA,a.Country,a.Market, a.BrandChapter,a.SubChannel,a.EventName,a.Event,a.Catlib,a.Beneficiary,a.Channel,a.Actual_Period,PeriodStartDate,PeriodEndDate,GRPs,a.MediaChannel ,a.ConsumerBehavior"));

		// Query_19 Duration
		PCollectionTuple query11 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_18"), rec_18)
				.and(new TupleTag<BeamRecord>("Duration_Flag"), Duration_Flag);
		PCollection<BeamRecord> rec_19 = query11.apply(BeamSql.queryMulti(
				"select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event, a.Catlib, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.BrandChapter,a.GRPs, a.CopyStartWithinPeriod ,a.CopyEndWithinPeriod, a.MediaChannel ,a.ConsumerBehavior, SUM(b.Flag) as Duration,SUM(b.contiflag) as Conti1 from rec_18 as a \r\n"
						+ "        left join Duration_Flag b \r\n" + "        on a.Channel = b.Outlet and \r\n"
						+ "        a.Event = b.EventidA \r\n"
						//+ "WHERE \r\n"
						//+ "        b.Weekint >= cast(cast(EXTRACT(YEAR from a.PeriodStartDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) END) as BIGINT) and \r\n"
						//+ "		   b.Weekint <= cast(cast(EXTRACT(YEAR from a.PeriodEndDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) END) as BIGINT) \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.SubChannel, a.EventName, a.Event, a.Catlib, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.BrandChapter,a.GRPs, a.CopyStartWithinPeriod ,a.CopyEndWithinPeriod, a.MediaChannel ,a.ConsumerBehavior "));

		PCollection<BeamRecord> rec_19_test  = rec_19
				.apply(BeamSql.query("Select SourceBDA,Country,Market, SubChannel, EventName, Event, Catlib, Beneficiary, Channel, Actual_Period, \r\n"  
					+	" PeriodStartDate, PeriodEndDate, BrandChapter,GRPs, CopyStartWithinPeriod ,CopyEndWithinPeriod, MediaChannel ,ConsumerBehavior,  Duration, Conti1,CAST(rec_19_testUDF(CAST(Conti1 AS DOUBLE),CAST(Duration AS DOUBLE)) AS DOUBLE) as Conti2 FROM PCOLLECTION").withUdf("rec_19_testUDF", rec_19_testUDF.class));
		

		
//		// Query_20temp
//		PCollectionTuple query12temp = PCollectionTuple.of(new TupleTag<BeamRecord>("hispanic_6"), hispanic_6)
//				.and(new TupleTag<BeamRecord>("rec_19"), rec_19);
//		PCollection<BeamRecord> rec_20temp = query12temp.apply(BeamSql.queryMulti("select a.*,b.Duration \r\n"
//				+ "       from hispanic_6 as a \r\n" + "       left join rec_19 as b \r\n"
//				+ "       on b.Channel = a.Outlet and \r\n" + "       a.SalesComponent = b.Event \r\n"
//				+ " group by a.TVHousehold,a.EvntKey,a.Iteration, a.PrimaryCausalKey,a.SourceBDA,a.Dueto_value,a.ProdKey,a.Country,a.Market, a.BrandChapter, a.SubChannel,a.EventName,Event,a.Catlib,a.Beneficiary,Channel,a.Actual_Period, \r\n"
//				+ "  PeriodStartDate,PeriodEndDate,CopyStartWithinPeriod,CopyEndWithinPeriod,GRPs,Duration,Week,a.MediaChannel,a.ConsumerBehavior,a.SalesComponent,a.Causal_Value_New,a.Outlet,b.Duration"));
//		// Query_20temp2
//		PCollection<BeamRecord> rec_20temp2  = rec_20temp
//				.apply(BeamSql.query("Select Outlet,Duration,SalesComponent, Beneficiary,CASE WHEN Duration > 0 THEN (Count(DISTINCT(Week))/Duration)*100 ELSE 0 END as Conti1 \r\n"
//						+ "	 from PCOLLECTION where Causal_Value_New > 20.0 \r\n"
//						+ "    group by Outlet,SalesComponent, Beneficiary,Duration"));

	//	PCollection<BeamRecord> rec_20temp3 = rec_20temp2.apply(BeamSql.query("Select Outlet,SalesComponent, Beneficiary, (CASE WHEN Duration > 0 THEN (Conti11/Duration)*100 ELSE 0 END) as Conti1 from PCOLLECTION"));
		
//		PCollection<BeamRecord> C_1  = hispanic_6
//		.apply(BeamSql.query("Select Outlet,SalesComponent, Beneficiary,Count(DISTINCT(Week)) as Conti1 \r\n"
//				+ "	 from PCOLLECTION where Causal_Value_New > 20.0 \r\n"
//				+ "    group by Outlet,SalesComponent, Beneficiary"));
//
//		
//		PCollectionTuple query_c1_join = PCollectionTuple.of(new TupleTag<BeamRecord>("C_1"), C_1)
//				.and(new TupleTag<BeamRecord>("rec_19"), rec_19);
//		PCollection<BeamRecord> C_2 = query_c1_join.apply(BeamSql.queryMulti(
//				"select a.*,b.Duration as Duration from C_1 as a left join rec_19 as b on b.Channel = a.Outlet and \r\n"  
//			  + "b.Event = a.SalesComponent "));
//
//		//group by a.Conti1,Outlet, SalesComponent, a.Beneficiary, b.Duration
//		PCollection<BeamRecord> C_3  = C_2
//				.apply(BeamSql.query("Select *,(cast(Conti1 as DOUBLE)/cast(Duration as DOUBLE)*cast('100.00' as DOUBLE)) as Conti2 FROM PCOLLECTION"));
//		
//
//		
//		// Query_20
//		// Continuity
//	
//		PCollectionTuple query_c3_join = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_19"), rec_19)
//				.and(new TupleTag<BeamRecord>("C_3"), C_3);
		PCollection<BeamRecord> rec_20 = rec_19_test.apply(BeamSql.query(
				"select *,Case when MediaChannel <> 'Digital' and Conti2 is null then cast('0.00' as DOUBLE) \r\n" + 
				"when MediaChannel <> 'Digital' and Conti2 is not null then Conti2 \r\n" + 
				"  when MediaChannel = 'Digital' Then cast('100.00' as DOUBLE) \r\n" + 
				"  end as Continuity \r\n" + 
				"        from PCOLLECTION "));
				

		// Query_21 Vehicle
		PCollection<BeamRecord> rec_21 = rec_20.apply(BeamSql
				.query("Select SourceBDA,Country,Market,CatLib, SubChannel,Beneficiary,Channel,Actual_Period,\r\n"
						+ "        PeriodStartDate,PeriodEndDate, BrandChapter, MediaChannel ,ConsumerBehavior from PCOLLECTION \r\n"
						+ "        group by SourceBDA,Country,Market, CatLib, SubChannel,Beneficiary,Channel,Actual_Period, PeriodStartDate,PeriodEndDate, BrandChapter,MediaChannel ,ConsumerBehavior"));

		
		// Query_22 Vechile_Grp
		PCollectionTuple query13 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_21"), rec_21)
				.and(new TupleTag<BeamRecord>("rec_17"), rec_17);
		PCollection<BeamRecord> rec_22 = query13.apply(BeamSql.queryMulti(
				" select a.SourceBDA,a.Country,a.Market, a.SubChannel,a.CatLib, a.Beneficiary, a.Channel,a.Actual_Period, \r\n"
						+ "	     a.PeriodStartDate,a.PeriodEndDate,a.MediaChannel, a.BrandChapter,a.ConsumerBehavior, SUM(b.GRPs) as GRPs from rec_21 as a \r\n"
						+ "        left join rec_17 as b\r\n" + "        on a.Channel = b.Channel and\r\n"
						+ "        a.Beneficiary = b.Beneficiary and \r\n"
						+ "        a.SubChannel = b.SubChannel and\r\n" + "        a.Market = b.Market \r\n" + "and a.MediaChannel=b.MediaChannel \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.CatLib,a.BrandChapter,a.SubChannel,a.Beneficiary,a.Channel,a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate,a.MediaChannel ,a.ConsumerBehavior "));
		

//		// Query_23 Vehicle_Duration
		PCollectionTuple query14 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_22"), rec_22)
				.and(new TupleTag<BeamRecord>("rec_20"), rec_20);
		PCollection<BeamRecord> rec_23 = query14.apply(BeamSql.queryMulti(
				" Select a.SourceBDA,a.Country, a.CatLib,a.Market, a.SubChannel, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate,a.PeriodEndDate, a.GRPs, a.MediaChannel,a.BrandChapter, a.ConsumerBehavior, max(b.Duration) as Duration, max(b.Continuity) as Continuity  from rec_22  a left join \r\n"
						+ "        rec_20 b on\r\n" + "        a.SubChannel = b.SubChannel and \r\n" + "a.MediaChannel = b.MediaChannel and \r\n"
						+ "        a.Market = b.Market and \r\n" + "        a.Actual_Period = b.Actual_Period \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.CatLib,a.SubChannel, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs,a.MediaChannel ,a.BrandChapter, a.ConsumerBehavior"));
		

		// Query_24 Vehicle_Spend
		PCollectionTuple query15 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_23"), rec_23)
				.and(new TupleTag<BeamRecord>("rec_6"), rec_6);
		PCollection<BeamRecord> rec_24 = query15.apply(BeamSql.queryMulti(
				" Select a.SourceBDA,a.Country,a.Market, a.CatLib,a.SubChannel, a.Beneficiary, a.Channel,a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "	 a.Duration, a.Continuity,a.MediaChannel ,a.ConsumerBehavior, b.BrandChapter,SUM(ReportedSpend) as ReportedSpend ,SUM(ModeledSpend) as ModeledSpend \r\n"
						+ "    from rec_23 a INNER JOIN rec_6 b on \r\n" + " a.SubChannel = b.SubChannel and \r\n"  
						+ "  a.Market = b.Market and a.BrandChapter = b.BrandChapter \r\n" + "and a.MediaChannel = b.Channel \r\n"
						+ "    group by a.SourceBDA,a.Country,a.Market, a.CatLib,a.SubChannel, Beneficiary, a.Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "	 a.Duration, a.Continuity,b.BrandChapter,a.MediaChannel ,a.ConsumerBehavior"));

		
		// Query_25 Vehicle_Dueto
		PCollectionTuple query16 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_24"), rec_24)
				.and(new TupleTag<BeamRecord>("rec_13"), rec_13);
		PCollection<BeamRecord> rec_25 = query16.apply(BeamSql.queryMulti(
				" select a.SourceBDA,a.Country, a.Market, a.CatLib,a.SubChannel, a.Beneficiary, a.Channel,a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "		 a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.MediaChannel, a.ConsumerBehavior ,SUM(b.Dueto_value) as DuetoVolume from rec_24  a \r\n"
						+ "        left join rec_13 b \r\n" + "on a.Channel   = b.Outlet and \r\n"
						+ "        a.Beneficiary = b.Beneficiary and \r\n" + "a.SubChannel = b.SubChannel and a.Market = b.Market and a.MediaChannel=b.MediaChannel \r\n"
						+ "        where cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) >= cast(cast(EXTRACT(YEAR from a.PeriodStartDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) END) as BIGINT) and \r\n"
						+ "		 cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) <= cast(cast(EXTRACT(YEAR from a.PeriodEndDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) END) as BIGINT) \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.CatLib,a.SubChannel, a.Beneficiary, a.Channel,a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "		 a.Duration, a.Continuity, a.ReportedSpend, a.ModeledSpend, a.BrandChapter,a.MediaChannel ,a.ConsumerBehavior"));


		// Query_26 and a.Actual_Period = b.Period removed
		PCollectionTuple query17 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_25"), rec_25)
				.and(new TupleTag<BeamRecord>("shipment"), shipment);
		PCollection<BeamRecord> rec_26 = query17.apply(BeamSql.queryMulti(
				"select a.SourceBDA,a.Country,a.Market, a.CatLib,a.SubChannel, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,a.MediaChannel ,a.ConsumerBehavior,  b.ProjectionFactor as ProjectionFactor,b.ChannelVolume as ChannelVolume, b.AllOutletVolume as AllOutletVolume from rec_25 a \r\n"
						+ "        left join shipment b ON a.Beneficiary = b.Beneficiary \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.SubChannel,a.CatLib, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend, a.ModeledSpend, a.DuetoVolume, a.MediaChannel ,a.ConsumerBehavior,b.ProjectionFactor,b.AllOutletVolume,b.ChannelVolume "));
		
		// Query_27 Vehicle_Projection
		PCollection<BeamRecord> rec_27 = rec_26.apply(BeamSql.query(
				"select SourceBDA,Country,Market, CatLib,SubChannel, Beneficiary, Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "Duration, Continuity, BrandChapter, ReportedSpend , ModeledSpend, DuetoVolume, ProjectionFactor,ChannelVolume,AllOutletVolume, \r\n"
						+ "(Case when ProjectionFactor = cast('0.00' as double) or ProjectionFactor is null then cast('0.00' as double) \r\n"
						+ "when ReportedSpend <> cast('0.00' as double) then ((DuetoVolume/(ModeledSpend/ReportedSpend))/ProjectionFactor) else (DuetoVolume/ProjectionFactor) end) as Volume, MediaChannel, ConsumerBehavior from PCOLLECTION"));

		
		// Query_28 Vehicle_Curves
		PCollectionTuple query18 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_27"), rec_27)
				.and(new TupleTag<BeamRecord>("curve"), curve);
		PCollection<BeamRecord> rec_28 = query18.apply(BeamSql.queryMulti(
				" Select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.CatLib, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,  a.ProjectionFactor , a.ChannelVolume, a.AllOutletVolume, a.Volume, a.MediaChannel ,a.ConsumerBehavior, AVG(b.Alpha) as Alpha , AVG(b.Beta) as Beta \r\n"
						+ "        from rec_27 a left join \r\n" + "        curve b on \r\n"
						+ "        a.BrandChapter = b.Brand and \r\n" + "        a.SubChannel = b.SubChannel and \r\n"
						+ "        a.Market = b.Market \r\n"
						+ "        group by a.SourceBDA,a.Country,a.Market, a.SubChannel, a.CatLib, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,  a.ProjectionFactor , a.ChannelVolume, a.AllOutletVolume, a.Volume,MediaChannel ,ConsumerBehavior"));

		
//		// Query_29 Vehicle_Financials
		PCollectionTuple query19 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_28"), rec_28)
				.and(new TupleTag<BeamRecord>("financials"), financials);
		PCollection<BeamRecord> rec_29 = query19.apply(BeamSql.queryMulti(
				" Select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.CatLib, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend, a.ModeledSpend, a.DuetoVolume, a.ProjectionFactor,a.ChannelVolume, a.AllOutletVolume, a.Volume, a.Alpha ,a.Beta,a.MediaChannel ,a.ConsumerBehavior ,b.rNR, b.rNCS, b.rCtb, b.rAC from rec_28 as a \r\n"
						+ "left join financials as b on a.Beneficiary = b.BeneficiaryFinance"));
		
		PCollection<BeamRecord> TEST_BRANDSH = brandsH.apply(BeamSql.query(
				" Select distinct Studio, Neighborhoods, BU, Division, Brand_Chapter from PCOLLECTION"));
		
		// Query_30 Vehicle_Mapped
		PCollectionTuple query20 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_29"), rec_29)
				.and(new TupleTag<BeamRecord>("TEST_BRANDSH"), TEST_BRANDSH);
		PCollection<BeamRecord> rec_30 = query20.apply(BeamSql.queryMulti(
				"Select a.SourceBDA,a.Country,a.Market, a.SubChannel, a.CatLib, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "		a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,  a.ProjectionFactor, a.ChannelVolume, a.AllOutletVolume, a.Volume, a.Alpha ,a.Beta, a.rNR, a.rNCS, a.rCtb, a.rAC,'Total 52 week' as EventName, 'Vehicle' as Level, a.MediaChannel ,a.ConsumerBehavior, b.Studio, b.Neighborhoods, b.BU, b.Division from rec_29 as a \r\n"
						+ "		left join TEST_BRANDSH as b on a.BrandChapter = b.Brand_Chapter"));



		
		PCollection<BeamRecord> rec_30_Basis1Temp = efficiency.apply(BeamSql.query(
				"SELECT BrandChapter,Level, ActualPeriod, MediaChannel, Market, Spend, Duration from PCOLLECTION"));
		


		// Basis- temp
		PCollection<BeamRecord> rec_30_Basis1TempX = rec_30_Basis1Temp
				.apply(BeamSql.query("SELECT DISTINCT(BrandChapter) as Chapter,Level, ActualPeriod as BasisPeriod, \r\n"
						+ "	MediaChannel as BasisChannel, Market as BasisMarket, Spend, cast(SUBSTRING(ActualPeriod FROM 3 FOR 2) as INTEGER) as Year2, max(Duration) as BasisDuration, \r\n"
						+ "	(CASE WHEN SUBSTRING(ActualPeriod FROM 1 FOR 2) = 'FY' THEN 1 else 0 end) as Marker from PCOLLECTION \r\n"
						+ "  WHERE Level = 'Vehicle' group by Level, BrandChapter, ActualPeriod, MediaChannel, Market, Spend,Duration"));

		// Basis-1
		PCollection<BeamRecord> rec_30_Basis1 = rec_30_Basis1TempX
				.apply(BeamSql.query("SELECT * from PCOLLECTION WHERE Marker = 1"));

		
		// Basis-2
		PCollection<BeamRecord> rec_30_Basis2 = rec_30.apply(BeamSql.query(
				"SELECT SourceBDA,Country,Market,CatLib, SubChannel, Beneficiary, Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "  Duration, Continuity, BrandChapter, ReportedSpend , ModeledSpend, DuetoVolume,  ProjectionFactor ,ChannelVolume, AllOutletVolume, Volume, Alpha ,Beta, rNR, rNCS, rCtb, rAC,EventName, \r\n"
						+ " Level, MediaChannel ,ConsumerBehavior, Studio, Neighborhoods, BU, Division, CAST(SUBSTRING(Actual_Period FROM 3 FOR 2) as INTEGER) as PeriodX from PCOLLECTION"));
		
		// Basis-3 temp
		PCollectionTuple queryBasis1 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_30_Basis2"), rec_30_Basis2)
				.and(new TupleTag<BeamRecord>("rec_30_Basis1"), rec_30_Basis1);
		PCollection<BeamRecord> rec_30_Basis3Temp = queryBasis1.apply(BeamSql.queryMulti(
				"SELECT a.SourceBDA,a.Country,a.Market,a.CatLib, a.SubChannel, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend, a.ModeledSpend, a.DuetoVolume, a.ProjectionFactor,a.ChannelVolume, a.AllOutletVolume, a.Volume, a.Alpha ,a.Beta, a.rNR, a.rNCS, a.rCtb, a.rAC, a.EventName, a.Level,a.MediaChannel ,a.ConsumerBehavior, a.Studio, a.Neighborhoods, a.BU, a.Division, a.PeriodX, b.year2, b.Spend, b.BasisDuration \r\n"
						+ "FROM  rec_30_Basis2 as a \r\n"
						+ "Left JOIN  rec_30_Basis1 as b ON a.BrandChapter = b.Chapter \r\n"
						+ "and a.MediaChannel = b.BasisChannel and a.Market = b.BasisMarket"));
		

		PCollection<BeamRecord> test = rec_30_Basis3Temp.apply(BeamSql.query(
				"SELECT SourceBDA, Country, CatLib,BasisDuration,Market, SubChannel, Beneficiary, Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "	Duration, Continuity, BrandChapter, ReportedSpend , ModeledSpend, DuetoVolume, ProjectionFactor, ChannelVolume, AllOutletVolume,Volume, Spend,Alpha ,Beta, rNR, rNCS, rCtb, rAC,EventName, \r\n"
						+ "	Level, MediaChannel, ConsumerBehavior, Studio, Neighborhoods, BU, Division, Year2, MAX(PeriodX) as maxPeriod from PCOLLECTION group by SourceBDA,CatLib,Country,Market, SubChannel, Beneficiary, Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "	Duration, Continuity, BrandChapter, ReportedSpend, ModeledSpend, DuetoVolume, ProjectionFactor ,ChannelVolume, AllOutletVolume,"
						+ "Volume, Alpha ,Beta, rNR, rNCS, rCtb, rAC,EventName, \r\n"
                        + "	Level, MediaChannel ,ConsumerBehavior, Studio, Neighborhoods, BU, Division, Year2, Spend, BasisDuration"));

//		// Basis-3
	PCollection<BeamRecord> rec_30_Basis3 = test.apply(BeamSql.query(
				"SELECT SourceBDA, Country, Market,CatLib, SubChannel, Beneficiary, Channel, Actual_Period, PeriodStartDate, PeriodEndDate, GRPs, \r\n"
						+ "	Duration, Continuity, BrandChapter, ReportedSpend , ModeledSpend, DuetoVolume, ProjectionFactor,ChannelVolume, AllOutletVolume,Volume, Alpha ,Beta, rNR, rNCS, rCtb, rAC,EventName, \r\n"
						+ "	Level, MediaChannel, ConsumerBehavior, Studio, Neighborhoods, BU, Division, Year2, \r\n"
						+ "	(CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'CY' and maxPeriod = Year2  THEN Spend \r\n"
						+ "           WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) ='FY' and maxPeriod-1 =Year2 THEN Spend \r\n"
						+ "             ElSE cast('0.00' as DOUBLE)  END) as BASIS_PY, \r\n"
						+ "  (CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'CY' and maxPeriod-1 = Year2 THEN Spend \r\n"
						+ "       WHEN  SUBSTRING(Actual_Period FROM 1 FOR 2) = 'FY' and maxPeriod-2 = Year2 THEN Spend \r\n"
						+ "             ELSE cast('0.00' as DOUBLE)  END ) as BASIS_P2Y, \r\n"
						+ "  (CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) ='CY' and  maxPeriod-2 = Year2 THEN Spend \r\n"
						+ "           WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'FY' and maxPeriod-3 = Year2 THEN Spend \r\n"
						+ "             ELSE cast('0.00' as DOUBLE) END) as BASIS_P3Y, \r\n"
						+ "  (CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'CY' and maxPeriod = Year2 THEN BasisDuration \r\n"
						+ "           WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'FY' and maxPeriod-1 = Year2 THEN BasisDuration \r\n"
						+ "           ELSE cast('0.00' as DOUBLE) END) as BASIS_Duration_PY, \r\n"
						+ "  (CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'CY' and maxPeriod-1 = Year2 THEN BasisDuration \r\n"
						+ "           WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'FY' and maxPeriod-2 = Year2 THEN BasisDuration \r\n"
						+ "           ELSE cast('0.00' as DOUBLE) END) as BASIS_Duration_P2Y, \r\n"
						+ "  (CASE WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) ='CY' and maxPeriod-2 = Year2 THEN BasisDuration \r\n"
						+ "           WHEN SUBSTRING(Actual_Period FROM 1 FOR 2) = 'FY' and maxPeriod-3 = Year2 THEN BasisDuration \r\n"
						+ "           ELSE cast('0.00' as DOUBLE) END) as BASIS_Duration_P3Y from PCOLLECTION "));
		
		
//		// VehicleBasis Removed aggregations and group by
	PCollection<BeamRecord> rec_30_BasisAgg = rec_30_Basis3.apply(BeamSql.query("SELECT SourceBDA,CatLib,Country,Market,SubChannel,MediaChannel,ConsumerBehavior,BrandChapter,Beneficiary,Channel, \r\n" + 
					"Actual_Period,PeriodStartDate,PeriodEndDate,GRPs,Duration,Continuity,ReportedSpend,ModeledSpend,Level,DuetoVolume, \r\n" + 
					"ProjectionFactor,ChannelVolume,AllOutletVolume,Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName,Studio,Neighborhoods,BU,Division, \r\n" + 
					" SUM(BASIS_PY) as BASIS_PY, SUM(BASIS_P2Y) as BASIS_P2Y, SUM(BASIS_P3Y) as BASIS_P3Y, \r\n" + 
					" SUM(BASIS_Duration_PY) as BASIS_Duration_PY,SUM(BASIS_Duration_P2Y) as BASIS_Duration_P2Y,SUM(BASIS_Duration_P3Y) as BASIS_Duration_P3Y, \r\n" + 
					"Case when MediaChannel = 'Digital' then cast('75.00' as double) \r\n" +
					"when MediaChannel = 'Linear TV' and Market = 'Hispanic' then cast('60.00' as double) \r\n" +
					"when MediaChannel = 'Linear TV' and Market = 'GM' then cast('90.00' as double) \r\n" +
					"when MediaChannel = 'Radio' then cast('12.50' as double) \r\n" +
					"when MediaChannel = 'Print' then cast('12.50' as double) end as Typical from PCOLLECTION \r\n"
						+ "GROUP BY SourceBDA,CatLib,Country,Market,SubChannel,MediaChannel,ConsumerBehavior,BrandChapter,Beneficiary,Channel,Actual_Period,PeriodStartDate, \r\n"
						+ "PeriodEndDate,GRPs,Duration,Continuity,ReportedSpend,ModeledSpend,Level,DuetoVolume, ProjectionFactor,ChannelVolume, AllOutletVolume,Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName, \r\n"
						+ "Studio,Neighborhoods,BU,Division,MediaChannel"));

	
//		
//		// Cont queries
//		// Vehicle_Cont1
		PCollection<BeamRecord> Cont1 = rec_30_BasisAgg.apply(BeamSql.query(
				"select *,(case when (GRPs/Typical) <= Duration then GRPs/Typical else Duration end) as Cont1 from PCOLLECTION"));

	
		// Vehicle_Cont2 take only integer part of Cont1
		PCollection<BeamRecord> Cont2 = Cont1.apply(BeamSql.query(
				"Select *, (case when Cont1 >= cast('1.00' as DOUBLE) then cast(Cont1 as INTEGER) else cast('1.00' as DOUBLE) end) as Cont2 from PCOLLECTION"));

		

		// Vehicle_Cont3
		PCollection<BeamRecord> Cont3 = Cont2.apply(BeamSql.query(
				"Select *, (case when Duration = cast('0.00' as DOUBLE) or Duration is null then cast('0.00' as DOUBLE) else (Cont2/Duration)*100 end) as Cont3 from PCOLLECTION"));

		
		// Vehicle_Cont4
		PCollection<BeamRecord> Cont4 = Cont3.apply(BeamSql.query(
				"Select *, (case when Continuity is null OR MediaChannel <> 'Linear TV' then Cont3 else Continuity end) as Cont from PCOLLECTION"));
		
	
//		PCollection<BeamRecord> temp_cal = Cont4.apply(BeamSql.query(
//				"SELECT *,GRPs/Duration/Cont*BASIS_PY*Duration/BASIS_Duration_PY/ReportedSpend as X1, \r\n"
//						+ " PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, ReportedSpend, ModeledSpend, DuetoVolume,ProjectionFactor, \r\n"
//						+ " Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName,Level,Studio,Neighborhoods,BU,Division,BASIS_PY,BASIS_P2Y, \r\n"
//						+ " BASIS_P3Y,BASIS_Duration_PY,BASIS_Duration_P2Y,BASIS_Duration_P3Y,Typical, Cont, \r\n"
//						+ " Volume*rAC as AC ,CASE WHEN Duration = cast('0.00' as double) THEN cast('0.00' as double) ELSE (GRPs/Duration/Cont*BASIS_PY*Duration/BASIS_Duration_PY/ReportedSpend) END as X1, \r\n"
//						+ "CASE WHEN Duration = cast('0.00' as double) THEN cast('0.00' as double) ELSE (GRPs/Duration/Cont*BASIS_P2Y*Duration/BASIS_Duration_P2Y/ReportedSpend) END as X1_2, \r\n"
//						+ "CASE WHEN Duration = cast('0.00' as double) THEN cast('0.00' as double) ELSE (GRPs/Duration/Cont*BASIS_P3Y*Duration/BASIS_Duration_P3Y/ReportedSpend) END as X1_3, \r\n"
//						+ "CASE WHEN Duration = cast('0.00' as double) THEN cast('0.00' as double) ELSE (GRPs/Duration/Cont) END as X2, 'CalenderYear' as PeriodType, Actual_Period as Report_Period, CatLib from PCOLLECTION"));



		// Calculated fields Filter removed
		PCollection<BeamRecord> rec_30_BasisCalc = Cont4.apply(BeamSql.query(
				"SELECT SourceBDA,Country,Market,SubChannel,MediaChannel,ConsumerBehavior,BrandChapter,Beneficiary,Channel,Actual_Period, \r\n"
						+ " PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, ReportedSpend, ModeledSpend, DuetoVolume,ProjectionFactor, \r\n"
						+ " Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName,Level,Studio,Neighborhoods,BU,Division,BASIS_PY,BASIS_P2Y, \r\n"
						+ " BASIS_P3Y,BASIS_Duration_PY,BASIS_Duration_P2Y,BASIS_Duration_P3Y,Typical, Cont, \r\n"
						+ " Volume*rAC as AC ,durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_PY as DOUBLE),CAST(BASIS_Duration_PY AS DOUBLE),cast(ReportedSpend as DOUBLE))as X1, \r\n" 
						+ "durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_P2Y as DOUBLE),CAST(BASIS_Duration_P2Y AS DOUBLE),cast(ReportedSpend as DOUBLE)) as X1_2, \r\n"
					    + "durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_P3Y as DOUBLE),CAST(BASIS_Duration_P3Y AS DOUBLE),cast(ReportedSpend as DOUBLE)) as X1_3, \r\n"
						+ " X2UDF(cast(GRPs as DOUBLE),cast(Duration as DOUBLE),cast(Cont as DOUBLE)) as X2, 'CalenderYear' as PeriodType, Actual_Period as Report_Period, CatLib,ChannelVolume,AllOutletVolume from PCOLLECTION ").withUdf("durationUDF", durationUDF.class).withUdf("X2UDF", X2UDF.class));

	
		// Converting Beam Record into Class type to implement Gamma function
		PCollection<VehicleGamma> rec_30Gamma = rec_30_BasisCalc.apply(ParDo.of(new DoFn<BeamRecord, VehicleGamma>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				BeamRecord record = c.element();
				String strArr = record.toString();
				String strArr1 = strArr.substring(24);
				String xyz = strArr1.replace("]", "");
				String[] strArr2 = xyz.split(",");
				
				double Gamma_X1_Output1 = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[41])); // True
				double Gamma_X1_2_Output2 = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[42])); // True
				double Gamma_X1_3_Output3 = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[43])); // True
				double Gamma_X2_Output4 = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[44])); // True

				VehicleGamma moc = new VehicleGamma();
				moc.setSourceBDA(strArr2[0]);
				moc.setCountry(strArr2[1]);
				moc.setMarket(strArr2[2]);
				moc.setSubChannel(strArr2[3]);
				moc.setMediaChannel(strArr2[4]);
				moc.setConsumerBehavior(strArr2[5]);
				moc.setBrandChapter(strArr2[6]);
				moc.setBeneficiary(strArr2[7]);
				moc.setChannel(strArr2[8]);
				moc.setActual_Period(strArr2[9]);
				moc.setPeriodStartDate(strArr2[10]);
				moc.setPeriodEndDate(strArr2[11]);
				moc.setGRPs(Double.parseDouble(strArr2[12]));
				moc.setDuration(Double.parseDouble(strArr2[13]));
				moc.setContinuity(Double.parseDouble(strArr2[14]));
				moc.setReportedSpend(Double.parseDouble(strArr2[15]));
				moc.setModeledSpend(Double.parseDouble(strArr2[16]));
				moc.setDuetoVolume(Double.parseDouble(strArr2[17]));
				moc.setProjectionFactor(Double.parseDouble(strArr2[18]));
				moc.setVolume(Double.parseDouble(strArr2[19]));
				moc.setAlpha(Double.parseDouble(strArr2[20]));
				moc.setBeta(Double.parseDouble(strArr2[21]));
				moc.setrNR(Double.parseDouble(strArr2[22]));
				moc.setrNCS(Double.parseDouble(strArr2[23]));
				moc.setrCtb(Double.parseDouble(strArr2[24]));
				moc.setrAC(Double.parseDouble(strArr2[25]));
				moc.setEventName(strArr2[26]);
				moc.setLevel(strArr2[27]);
				moc.setStudio(strArr2[28]);
				moc.setNeighborhoods(strArr2[29]);
				moc.setBU(strArr2[30]);
				moc.setDivision(strArr2[31]);
				moc.setBASIS_PY(Double.parseDouble(strArr2[32]));
				moc.setBASIS_P2Y(Double.parseDouble(strArr2[33]));
				moc.setBASIS_P3Y(Double.parseDouble(strArr2[34]));
				moc.setBASIS_Duration_PY(Double.parseDouble(strArr2[35]));
				moc.setBASIS_Duration_P2Y(Double.parseDouble(strArr2[36]));
				moc.setBASIS_Duration_P3Y(Double.parseDouble(strArr2[37]));
				moc.setTypical(Double.parseDouble(strArr2[38]));
				moc.setCont(Double.parseDouble(strArr2[39]));
				moc.setAC(Double.parseDouble(strArr2[40]));
				moc.setX1(Double.parseDouble(strArr2[41]));
				moc.setX1_2(Double.parseDouble(strArr2[42]));
				moc.setX1_3(Double.parseDouble(strArr2[43]));
				moc.setX2(Double.parseDouble(strArr2[44]));
				moc.setPeriodType(strArr2[45]);
				moc.setReport_Period(strArr2[46]);
				moc.setCatLib(strArr2[47]);
				moc.setChannelVolume(Double.parseDouble(strArr2[48]));
				moc.setAllOutletVolume(Double.parseDouble(strArr2[49]));
				moc.setGamma_X1(Gamma_X1_Output1);
				moc.setGamma_X1_2(Gamma_X1_2_Output2);
				moc.setGamma_X1_3(Gamma_X1_3_Output3);
				moc.setGamma_X2(Gamma_X2_Output4);
				c.output(moc);
			}
		}));

		List<String> fieldNamesG = Arrays.asList("SourceBDA", "Country", "Market", "SubChannel", "MediaChannel",
				"ConsumerBehavior", "BrandChapter", "Beneficiary", "Channel", "Actual_Period", "PeriodStartDate",
				"PeriodEndDate", "GRPs", "Duration", "Continuity", "ReportedSpend", "Level", "ModeledSpend",
				"DuetoVolume", "ProjectionFactor", "Volume", "Alpha", "Beta", "rNR", "rNCS", "rCtb", "rAC", "EventName",
				"Studio", "Neighborhoods", "BU", "Division", "BASIS_PY", "BASIS_P2Y", "BASIS_P3Y", "BASIS_Duration_PY",
				"BASIS_Duration_P2Y", "BASIS_Duration_P3Y", "Typical", "Cont", "AC", "X1", "X1_2", "X1_3", "X2",
				"PeriodType", "Report_Period", "CatLib","ChannelVolume","AllOutletVolume","Gamma_X1", "Gamma_X1_2", "Gamma_X1_3", "Gamma_X2");

		List<Integer> fieldTypesG = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,Types.DOUBLE);

		final BeamRecordSqlType appTypeG = BeamRecordSqlType.create(fieldNamesG, fieldTypesG);

		PCollection<BeamRecord> appsG = rec_30Gamma.apply(ParDo.of(new DoFn<VehicleGamma, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appTypeG, c.element().SourceBDA, c.element().Country, c.element().Market,
						c.element().SubChannel, c.element().MediaChannel, c.element().ConsumerBehavior,
						c.element().BrandChapter, c.element().Beneficiary, c.element().Channel,
						c.element().Actual_Period, c.element().PeriodStartDate, c.element().PeriodEndDate,
						c.element().GRPs, c.element().Duration, c.element().Continuity, c.element().ReportedSpend,
						c.element().Level, c.element().ModeledSpend, c.element().DuetoVolume, c.element().ProjectionFactor, 
						c.element().Volume, c.element().Alpha, c.element().Beta,
						c.element().rNR, c.element().rNCS, c.element().rCtb, c.element().rAC, c.element().EventName,
						c.element().Studio, c.element().Neighborhoods, c.element().BU, c.element().Division,
						c.element().BASIS_PY, c.element().BASIS_P2Y, c.element().BASIS_P3Y,
						c.element().BASIS_Duration_PY, c.element().BASIS_Duration_P2Y, c.element().BASIS_Duration_P3Y,
						c.element().Typical, c.element().Cont, c.element().AC, c.element().X1, c.element().X1_2,
						c.element().X1_3, c.element().X2, c.element().PeriodType, c.element().Report_Period,
						c.element().CatLib,c.element().ChannelVolume,c.element().AllOutletVolume, c.element().Gamma_X1, c.element().Gamma_X1_2, c.element().Gamma_X1_3,
						c.element().Gamma_X2);
				c.output(br);
				
			}
		})).setCoder(appTypeG.getRecordCoder());
		

			
		PCollection<BeamRecord> vehicle_gamma = appsG.apply(BeamSql.query(
				"Select *,GAMMAUDF(Gamma_X1,Gamma_X2,ReportedSpend,BASIS_Duration_PY,Duration,cast(BASIS_PY as DOUBLE)) as Xnorm1, \r\n" + 
				"GAMMAUDF(cast(Gamma_X1_2 as DOUBLE),CAST(Gamma_X2 as DOUBLE),cast(ReportedSpend as DOUBLE),cast(BASIS_Duration_P2Y as DOUBLE),CAST(Duration as DOUBLE),cast(BASIS_P2Y as DOUBLE)) as Xnorm2, \r\n" + 
				"GAMMAUDF(cast(Gamma_X1_3 as DOUBLE),CAST(Gamma_X2 as DOUBLE),cast(ReportedSpend as DOUBLE),cast(BASIS_Duration_P3Y as DOUBLE),CAST(Duration as DOUBLE),cast(BASIS_P3Y as DOUBLE)) as Xnorm3 \r\n" + 
				" from PCOLLECTION  ").withUdf("GAMMAUDF", GAMMAUDF.class));

//		PCollection<String> gs_output_finals = vehicle_gamma.apply(ParDo.of(new DoFn<BeamRecord, String>() {
//		private static final long serialVersionUID = 1L;
//
//		@ProcessElement
//		public void processElement(ProcessContext c) {
//			c.output(c.element().toString());
//			System.out.println(c.element().toString());
//		}
//	}));
//	gs_output_finals.apply(TextIO.write().to("gs://cloroxtegadeff/output/vehicle_gamma_c2"));

		
		
		// Query_31 Campaign starts from here
	PCollection<BeamRecord> rec_31 = rec_20.apply(BeamSql.query(
				"select SourceBDA, CatLib,Country, Market, BrandChapter, SubChannel, EventName, Beneficiary, Channel, Actual_Period,\r\n"
						+ "        PeriodStartDate, PeriodEndDate, MediaChannel, ConsumerBehavior from PCOLLECTION \r\n"
						+ "        group by Market, BrandChapter,SubChannel, Beneficiary, CatLib,Channel, Actual_Period, PeriodStartDate, PeriodEndDate, EventName,MediaChannel ,ConsumerBehavior,SourceBDA, Country"));

	
		// Query_32 Campaign_Grp
		PCollectionTuple query21 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_31"), rec_31)
				.and(new TupleTag<BeamRecord>("rec_17"), rec_17);
		PCollection<BeamRecord> rec_32 = query21.apply(BeamSql.queryMulti(
				" select  a.SourceBDA, a.Country,a.Market, a.SubChannel,a.CatLib, a.BrandChapter, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate,a.MediaChannel,a.ConsumerBehavior ,SUM(b.GRPs) as GRPs from rec_31 as a \r\n"
						+ "        left join rec_17 as b\r\n" + "        on a.Channel   = b.Channel and\r\n"
						+ "        a.beneficiary = b.beneficiary and \r\n" + "        a.EventName = b.EventName\r\n"
						+ "        group by a.SourceBDA, a.Country, a.CatLib,a.Market, a.BrandChapter, a.SubChannel, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate,a.MediaChannel ,a.ConsumerBehavior"));


		
		// Query_33 Campaign_Duration
		PCollectionTuple query22 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_32"), rec_32)
				.and(new TupleTag<BeamRecord>("rec_20"), rec_20);
		PCollection<BeamRecord> rec_33 = query22.apply(BeamSql.queryMulti(
				" Select a.SourceBDA, a.Country,a.CatLib,a.Market, a.SubChannel, a.BrandChapter, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs,a.MediaChannel,a.ConsumerBehavior,  max(b.Duration) as Duration, max(b.Continuity) as Continuity  from rec_32 a left join \r\n"
						+ "        rec_20 b \r\n" + "        on a.EventName=b.EventName and \r\n" + "a.MediaChannel = b.MediaChannel and \r\n"
						+ "        a.Actual_Period = b.Actual_Period and a.Market = b.Market and  a.SubChannel = b.SubChannel \r\n"
						+ "        group by a.SourceBDA, a.Country,a.Market, a.CatLib,a.BrandChapter, a.SubChannel, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs,a.MediaChannel ,a.ConsumerBehavior"));

		
		
		// Query_34 Campaign_Spend
		PCollectionTuple query23 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_33"), rec_33)
				.and(new TupleTag<BeamRecord>("rec_6"), rec_6);
		PCollection<BeamRecord> rec_34 = query23.apply(BeamSql.queryMulti(
				" Select a.SourceBDA, a.Country,a.Market, a.CatLib,a.SubChannel, a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "	  	a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity,a.MediaChannel, a.ConsumerBehavior, SUM(b.ReportedSpend) as ReportedSpend ,SUM(b.ModeledSpend) as ModeledSpend \r\n"
						+ "		from rec_33 a INNER JOIN rec_6 b on \r\n" + "       a.EventName = b.EventName and \r\n"
						+ "       a.BrandChapter = b.BrandChapter \r\n" + " and a.MediaChannel = b.Channel \r\n"
						+ " 		group by a.SourceBDA, a.Country,a.Market, a.BrandChapter,a.CatLib, a.SubChannel, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration , a.Continuity,a.MediaChannel ,a.ConsumerBehavior"));
		
		// Query_35 Campaign_Dueto
		PCollectionTuple query24 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_34"), rec_34)
				.and(new TupleTag<BeamRecord>("rec_13"), rec_13);
		PCollection<BeamRecord> rec_35 = query24.apply(BeamSql.queryMulti(
				" select  a.SourceBDA, a.Country,a.Market, a.SubChannel, a.CatLib, a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend,a.MediaChannel ,a.ConsumerBehavior ,SUM(b.Dueto_value)  as DuetoVolume from rec_34  a \r\n"
						+ "        left join rec_13 b \r\n" + "        on a.Channel = b.Outlet and \r\n"
						+ "        a.Beneficiary = b.Beneficiary and \r\n" + "        a.EventName = b.EventName \r\n"
						+ "        where cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) >= cast(cast(EXTRACT(YEAR from a.PeriodStartDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodStartDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodStartDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodStartDate) as VARCHAR) END) as BIGINT) and \r\n"
						+ "		 cast(cast(EXTRACT(YEAR from b.Week) as VARCHAR) || (case when EXTRACT(MONTH from b.Week) < 10 then '0' || cast(EXTRACT(MONTH from b.Week) as VARCHAR) else cast(EXTRACT(MONTH from b.Week) as VARCHAR) END) || (case when EXTRACT(DAY from b.Week) < 10 then '0' || cast(EXTRACT(DAY from b.Week) as VARCHAR) else  cast(EXTRACT(DAY from b.Week) as VARCHAR) END) as BIGINT) <= cast(cast(EXTRACT(YEAR from a.PeriodEndDate) as VARCHAR) || (case when EXTRACT(MONTH from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) else cast(EXTRACT(MONTH from a.PeriodEndDate) as VARCHAR) END) || (case when EXTRACT(DAY from a.PeriodEndDate) < 10 then '0' || cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) else  cast(EXTRACT(DAY from a.PeriodEndDate) as VARCHAR) END) as BIGINT) \r\n"
						+ "        group by a.SourceBDA, a.Country,a.Market, a.CatLib,a.SubChannel, a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.MediaChannel,a.ConsumerBehavior"));


		
		// Query_36 removed a.Actual_Period = b.Period
		PCollectionTuple query25 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_35"), rec_35)
				.and(new TupleTag<BeamRecord>("shipment"), shipment);
		PCollection<BeamRecord> rec_36 = query25.apply(BeamSql.queryMulti(
				" select a.SourceBDA, a.Country,a.Market, a.CatLib,a.SubChannel, a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume, MediaChannel,ConsumerBehavior, AVG(b.ProjectionFactor) as ProjectionFactor,AVG(b.ChannelVolume) as ChannelVolume, AVG(b.AllOutletVolume) as AllOutletVolume from rec_35 a \r\n"
						+ "        left join shipment b on a.Beneficiary = b.Beneficiary \r\n"
						+ "        group by a.SourceBDA, a.Country,a.Market, a.CatLib, a.SubChannel,a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,a.MediaChannel ,a.ConsumerBehavior"));

		
		// Query_37 Campaign_Projection
		PCollection<BeamRecord> rec_37 = rec_36.apply(BeamSql.query(
				"select SourceBDA, Country, CatLib,Market, SubChannel, EventName, BrandChapter, Beneficiary, Channel, Actual_Period, \r\n"
						+ "PeriodStartDate, PeriodEndDate, GRPs ,Duration, Continuity, ReportedSpend, ModeledSpend, DuetoVolume, ProjectionFactor,ChannelVolume,AllOutletVolume, \r\n"
						+ " (Case when ProjectionFactor = cast('0.00' as DOUBLE) or ProjectionFactor is null then cast('0.00' as DOUBLE) \r\n"
						+ " when ReportedSpend <> cast('0.00' as DOUBLE) then ((DuetoVolume/(ModeledSpend/ReportedSpend))/ProjectionFactor) else (DuetoVolume/ProjectionFactor) end) as Volume,MediaChannel ,ConsumerBehavior from PCOLLECTION"));

		
		//Query_38  Campaign_Curves
				PCollectionTuple query26 = PCollectionTuple.of(
						new TupleTag<BeamRecord>("rec_37"), rec_37).and(new TupleTag<BeamRecord>("curve"), curve);	    
				PCollection<BeamRecord> rec_38 = query26.apply(
						BeamSql.queryMulti("Select a.SourceBDA, a.Country,a.Market, a.SubChannel, a.CatLib,a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n" + 
								"	     a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume, a.ProjectionFactor,a.ChannelVolume,a.AllOutletVolume, a.Volume,a.MediaChannel ,a.ConsumerBehavior, AVG(b.Alpha) as Alpha , AVG(b.Beta) as Beta \r\n" + 
								"        from rec_37 a left join curve b on \r\n" + 
								"        a.BrandChapter = b.Brand and \r\n" + 
								"        a.SubChannel = b.SubChannel and \r\n" + 
								"        a.Market = b.Market \r\n" + 
								"        group by a.SourceBDA, a.Country,a.Market, a.SubChannel, a.CatLib, a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n" + 
								"		 a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume, a.ProjectionFactor,a.ChannelVolume,a.AllOutletVolume, a.Volume,a.MediaChannel,a.ConsumerBehavior"));

		// Query_39 Campaign_Financials
		PCollectionTuple query27 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_38"), rec_38)
				.and(new TupleTag<BeamRecord>("financials"), financials);
		PCollection<BeamRecord> rec_39 = query27.apply(BeamSql.queryMulti(
				"Select a.SourceBDA, a.Country,a.Market, a.SubChannel, a.CatLib,a.EventName, a.BrandChapter, a.Beneficiary, a.Channel, a.Actual_Period, \r\n"
						+ "		a.PeriodStartDate, a.PeriodEndDate, a.GRPs ,a.Duration, a.Continuity, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume, a.ProjectionFactor,a.ChannelVolume,a.AllOutletVolume,a.Volume, a.Alpha , a.Beta,a.MediaChannel ,a.ConsumerBehavior, b.rNR, b.rNCS, b.rCtb, b.rAC from rec_38 as a \r\n"
						+ "		left join financials as b on a.Beneficiary = b.BeneficiaryFinance"));


		
		// Query_40 Campaign_Mapped
		PCollectionTuple query28 = PCollectionTuple.of(new TupleTag<BeamRecord>("rec_39"), rec_39)
				.and(new TupleTag<BeamRecord>("TEST_BRANDSH"), TEST_BRANDSH);
		PCollection<BeamRecord> rec_40 = query28.apply(BeamSql.queryMulti(
				"Select a.SourceBDA, a.Country,a.Market, a.SubChannel, a.CatLib, a.EventName, a.Beneficiary, a.Channel, a.Actual_Period, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, \r\n"
						+ "		a.Duration, a.Continuity, a.BrandChapter, a.ReportedSpend , a.ModeledSpend, a.DuetoVolume,  a.ProjectionFactor ,a.ChannelVolume,a.AllOutletVolume, a.Volume, a.Alpha ,a.Beta, a.rNR, a.rNCS, a.rCtb, a.rAC, \r\n"
						+ " 		'Campaign' as Level,a.MediaChannel ,a.ConsumerBehavior, b.Studio, b.Neighborhoods, b.BU, b.Division from rec_39 as a \r\n"
						+ "		left join TEST_BRANDSH as b on a.BrandChapter = b.Brand_Chapter"));


		
		// Campaign_Basis
		PCollection<BeamRecord> rec_41 = rec_40.apply(BeamSql.query(
				"SELECT SourceBDA,Country,Market, CatLib, SubChannel,Actual_Period,MediaChannel,ConsumerBehavior,BrandChapter,Beneficiary,Channel,PeriodStartDate,PeriodEndDate,GRPs,Duration,Continuity,ReportedSpend,ModeledSpend,DuetoVolume,\r\n"
						+ "ProjectionFactor,ChannelVolume,AllOutletVolume,Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName,Level,Studio,Neighborhoods,BU,\r\n"
						+ "Division,(Case\r\n" + "when (MediaChannel = 'Digital' and Market = 'GM') Then 2000000 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'Hispanic') Then 3500000 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'GM') Then 7500000 else 0 end) as BASIS_PY,\r\n"
						+ "(Case\r\n" + "when (MediaChannel = 'Digital' and Market = 'GM') Then 2000000 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'Hispanic') Then 3500000 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'GM') Then 7500000 else 0 end) as BASIS_P2Y,\r\n"
						+ "(Case\r\n" + "when (MediaChannel = 'Digital' and Market = 'GM') Then 2000000 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'Hispanic') Then 3500000  \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'GM') Then 7500000 else 0 end) as BASIS_P3Y,\r\n"
						+ "(Case\r\n" + "when (MediaChannel = 'Digital') Then 75 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'Hispanic') Then 60 \r\n"
						+ "when (MediaChannel = 'Linear TV' and Market = 'GM') Then 90 \r\n"
						+ "when (MediaChannel = 'Radio') Then 12.50 \r\n"
						+ "when (MediaChannel = 'Print') Then 12.50 end) as Typical, 52 as BASIS_Duration_PY, 52 as BASIS_Duration_P2Y,52 as BASIS_Duration_P3Y from PCOLLECTION"));


		// Campaign_Cont1 removed Typical
	PCollection<BeamRecord> C_Cont1 = rec_41.apply(BeamSql
				.query("select *,cast(contUDF(cast(GRPs as DOUBLE),cast(Typical as DOUBLE),cast(Duration as DOUBLE)) as INTEGER) as Cont1 from PCOLLECTION").withUdf("contUDF", contUDF.class));

		// Campaign_Cont2 take only integer part of Cont1
		PCollection<BeamRecord> C_Cont2 = C_Cont1.apply(BeamSql.query(
				" Select *, (case when Cont1 >= cast('1.00' as DOUBLE) then Cont1 else cast('1.00' as DOUBLE) end) as Cont2 from PCOLLECTION"));

		
		// Campaign_Cont3
		PCollection<BeamRecord> C_Cont3 = C_Cont2.apply(BeamSql.query(
				" Select *, (case when Duration = cast('0.00' as DOUBLE) or Duration is null then cast('0.00' as DOUBLE) else (Cont2/Duration)*100 end) as Cont3 from PCOLLECTION"));
		
		// Campaign_Cont4
		PCollection<BeamRecord> C_Cont4 = C_Cont3.apply(BeamSql.query(
				" Select *, (case when Continuity is null OR MediaChannel <> 'Linear TV' then Cont3 else Continuity end) as Cont from PCOLLECTION"));


		
		// Calculated fields
	PCollection<BeamRecord> rec_42 = C_Cont4.apply(BeamSql.query(
				"SELECT SourceBDA,Country,Market,SubChannel,MediaChannel,ConsumerBehavior,BrandChapter,Beneficiary,Channel,Actual_Period, \r\n"
										+ " PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, ReportedSpend, ModeledSpend, DuetoVolume,ProjectionFactor, \r\n"
										+ " Volume,Alpha,Beta,rNR,rNCS,rCtb,rAC,EventName,Level,Studio,Neighborhoods,BU,Division,BASIS_PY,BASIS_P2Y, \r\n"
										+ " BASIS_P3Y,BASIS_Duration_PY,BASIS_Duration_P2Y,BASIS_Duration_P3Y,Typical, Cont, \r\n"
										+ " Volume*rAC as AC ,durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_PY as DOUBLE),CAST(BASIS_Duration_PY AS DOUBLE),cast(ReportedSpend as DOUBLE))as X1, \r\n" 
										+ "durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_P2Y as DOUBLE),CAST(BASIS_Duration_P2Y AS DOUBLE),cast(ReportedSpend as DOUBLE)) as X1_2, \r\n"
									    + "durationUDF(cast(GRPs as DOUBLE),CAST(Duration as DOUBLE),CAST(Cont as DOUBLE),cast(BASIS_P3Y as DOUBLE),CAST(BASIS_Duration_P3Y AS DOUBLE),cast(ReportedSpend as DOUBLE)) as X1_3, \r\n"
										+ " X2UDF(cast(GRPs as DOUBLE),cast(Duration as DOUBLE),cast(Cont as DOUBLE)) as X2, 'CalenderYear' as PeriodType, Actual_Period as Report_Period, CatLib,ChannelVolume,AllOutletVolume from PCOLLECTION ").withUdf("durationUDF", durationUDF.class).withUdf("X2UDF", X2UDF.class));


		// Converting Beam Record into Class type to implement Gamma function
	PCollection<CampaignGamma> rec_42Gamma = rec_42.apply(ParDo.of(new DoFn<BeamRecord, CampaignGamma>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				BeamRecord record = c.element();
				String strArr = record.toString();
				String strArr1 = strArr.substring(24);
				String xyz = strArr1.replace("]", "");
				String[] strArr2 = xyz.split(",");

				double Gamma_X1_Output = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[41])); // True
				double Gamma_X1_2_Output = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[42])); // True
				double Gamma_X1_3_Output = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[43])); // True
				double Gamma_X2_Output = new GammaDistribution(Double.parseDouble(strArr2[20]),
						Double.parseDouble(strArr2[21])).cumulativeProbability(Double.parseDouble(strArr2[44])); // True

				CampaignGamma moc = new CampaignGamma();
				moc.setSourceBDA(strArr2[0]);
				moc.setCountry(strArr2[1]);
				moc.setMarket(strArr2[2]);
				moc.setSubChannel(strArr2[3]);
				moc.setMediaChannel(strArr2[4]);
				moc.setConsumerBehavior(strArr2[5]);
				moc.setBrandChapter(strArr2[6]);
				moc.setBeneficiary(strArr2[7]);
				moc.setChannel(strArr2[8]);
				moc.setActual_Period(strArr2[9]);
				moc.setPeriodStartDate(strArr2[10]);
				moc.setPeriodEndDate(strArr2[11]);
				moc.setGRPs(Double.parseDouble(strArr2[12]));
				moc.setDuration(Double.parseDouble(strArr2[13]));
				moc.setContinuity(Double.parseDouble(strArr2[14]));
				moc.setReportedSpend(Double.parseDouble(strArr2[15]));
				moc.setModeledSpend(Double.parseDouble(strArr2[16]));
				moc.setDuetoVolume(Double.parseDouble(strArr2[17]));
				moc.setProjectionFactor(Double.parseDouble(strArr2[18]));
				moc.setVolume(Double.parseDouble(strArr2[19]));
				moc.setAlpha(Double.parseDouble(strArr2[20]));
				moc.setBeta(Double.parseDouble(strArr2[21]));
				moc.setrNR(Double.parseDouble(strArr2[22]));
				moc.setrNCS(Double.parseDouble(strArr2[23]));
				moc.setrCtb(Double.parseDouble(strArr2[24]));
				moc.setrAC(Double.parseDouble(strArr2[25]));
				moc.setEventName(strArr2[26]);
				moc.setLevel(strArr2[27]);
				moc.setStudio(strArr2[28]);
				moc.setNeighborhoods(strArr2[29]);
				moc.setBU(strArr2[30]);
				moc.setDivision(strArr2[31]);
				moc.setBASIS_PY(Double.parseDouble(strArr2[32]));
				moc.setBASIS_P2Y(Double.parseDouble(strArr2[33]));
				moc.setBASIS_P3Y(Double.parseDouble(strArr2[34]));
				moc.setBASIS_Duration_PY(Double.parseDouble(strArr2[35]));
				moc.setBASIS_Duration_P2Y(Double.parseDouble(strArr2[36]));
				moc.setBASIS_Duration_P3Y(Double.parseDouble(strArr2[37]));
				moc.setTypical(Double.parseDouble(strArr2[38]));
				moc.setCont(Double.parseDouble(strArr2[39]));
				moc.setAC(Double.parseDouble(strArr2[40]));
				moc.setX1(Double.parseDouble(strArr2[41]));
				moc.setX1_2(Double.parseDouble(strArr2[42]));
				moc.setX1_3(Double.parseDouble(strArr2[43]));
				moc.setX2(Double.parseDouble(strArr2[44]));
				moc.setPeriodType(strArr2[45]);
				moc.setReport_Period(strArr2[46]);
				moc.setCatLib(strArr2[47]);
				moc.setChannelVolume(Double.valueOf(strArr2[48]));
				moc.setAllOutletVolume(Double.parseDouble(strArr2[49]));
				moc.setGamma_X1(Gamma_X1_Output);
				moc.setGamma_X1_2(Gamma_X1_2_Output);
				moc.setGamma_X1_3(Gamma_X1_3_Output);
				moc.setGamma_X2(Gamma_X2_Output);
				c.output(moc);
			}
		}));

		List<String> fieldNamesG1 = Arrays.asList("SourceBDA", "Country", "Market", "SubChannel", "MediaChannel",
				"ConsumerBehavior", "BrandChapter", "Beneficiary", "Channel", "Actual_Period", "PeriodStartDate",
				"PeriodEndDate", "GRPs", "Duration", "Continuity", "ReportedSpend", "Level", "ModeledSpend",
				"DuetoVolume", "ProjectionFactor", "Volume", "Alpha", "Beta", "rNR", "rNCS", "rCtb", "rAC", "EventName",
				"Studio", "Neighborhoods", "BU", "Division", "BASIS_PY", "BASIS_P2Y", "BASIS_P3Y", "BASIS_Duration_PY",
				"BASIS_Duration_P2Y", "BASIS_Duration_P3Y", "Typical", "Cont", "AC", "X1", "X1_2", "X1_3", "X2",
				"PeriodType", "Report_Period", "CatLib","ChannelVolume","AllOutletVolume", "Gamma_X1", "Gamma_X1_2", "Gamma_X1_3", "Gamma_X2");

		List<Integer> fieldTypesG1 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR,
				Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE);

		final BeamRecordSqlType appTypeG1 = BeamRecordSqlType.create(fieldNamesG1, fieldTypesG1);

		PCollection<BeamRecord> appsG1 = rec_42Gamma.apply(ParDo.of(new DoFn<CampaignGamma, BeamRecord>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appTypeG1, c.element().SourceBDA, c.element().Country,
						c.element().Market, c.element().SubChannel, c.element().MediaChannel,
						c.element().ConsumerBehavior, c.element().BrandChapter, c.element().Beneficiary,
						c.element().Channel, c.element().Actual_Period, c.element().PeriodStartDate,
						c.element().PeriodEndDate, c.element().GRPs, c.element().Duration, c.element().Continuity,
						c.element().ReportedSpend, c.element().Level, c.element().ModeledSpend, c.element().DuetoVolume,
						c.element().ProjectionFactor, c.element().Volume, c.element().Alpha,
						c.element().Beta, c.element().rNR, c.element().rNCS, c.element().rCtb, c.element().rAC,
						c.element().EventName, c.element().Studio, c.element().Neighborhoods, c.element().BU,
						c.element().Division, c.element().BASIS_PY, c.element().BASIS_P2Y, c.element().BASIS_P3Y,
						c.element().BASIS_Duration_PY, c.element().BASIS_Duration_P2Y, c.element().BASIS_Duration_P3Y,
						c.element().Typical, c.element().Cont, c.element().AC, c.element().X1, c.element().X1_2,
						c.element().X1_3, c.element().X2, c.element().PeriodType, c.element().Report_Period,
						c.element().CatLib,c.element().ChannelVolume,c.element().AllOutletVolume, c.element().Gamma_X1, c.element().Gamma_X1_2, c.element().Gamma_X1_3,
						c.element().Gamma_X2);
				c.output(br);
			}
		})).setCoder(appTypeG1.getRecordCoder());
	
		PCollection<BeamRecord> campaign_gamma = appsG1.apply(BeamSql.query(
				"Select *,GAMMAUDF(cast(Gamma_X1 as DOUBLE),CAST(Gamma_X2 as DOUBLE),cast(ReportedSpend as DOUBLE),cast(BASIS_Duration_PY as DOUBLE),CAST(Duration as DOUBLE),cast(BASIS_PY as DOUBLE)) as Xnorm1, \r\n"
						+ "GAMMAUDF(cast(Gamma_X1_2 as DOUBLE),CAST(Gamma_X2 as DOUBLE),cast(ReportedSpend as DOUBLE),cast(BASIS_Duration_P2Y as DOUBLE),CAST(Duration as DOUBLE),cast(BASIS_P2Y as DOUBLE)) as Xnorm2, \r\n"
						+ "GAMMAUDF(cast(Gamma_X1_3 as DOUBLE),CAST(Gamma_X2 as DOUBLE),cast(ReportedSpend as DOUBLE),cast(BASIS_Duration_P3Y as DOUBLE),CAST(Duration as DOUBLE),cast(BASIS_P3Y as DOUBLE)) as Xnorm3 \r\n"
						+ " from PCOLLECTION  ")
				.withUdf("GAMMAUDF", GAMMAUDF.class));

		// convert BeamRecord int o String to get output in GS
//		PCollection<String> gs_output_final = campaign_gamma.apply(ParDo.of(new DoFn<BeamRecord, String>() {
//			private static final long serialVersionUID = 1L;
//
//			@ProcessElement
//			public void processElement(ProcessContext c) {
//				c.output(c.element().toString());
//				System.out.println(c.element().toString());
//			}
//		}));
//		gs_output_final.apply(TextIO.write().to("gs://cloroxtegadeff/output/campaign_gamma_c2"));
		
		// UNION ALL
		PCollectionTuple queryUnion = PCollectionTuple.of(new TupleTag<BeamRecord>("vehicle_gamma"), vehicle_gamma)
				.and(new TupleTag<BeamRecord>("campaign_gamma"), campaign_gamma);
		PCollection<BeamRecord> AppendQuery = queryUnion.apply(BeamSql.queryMulti(
				"select Level,Country,PeriodType,Report_Period,Actual_Period,SourceBDA,BrandChapter,Studio,Neighborhoods,BU,Division,SubChannel,MediaChannel, \r\n "
				+ "Market,ConsumerBehavior,EventName,'Yes' as Published,ReportedSpend as Spend,GRPs,Duration,Continuity,BASIS_PY,BASIS_P2Y,BASIS_P3Y, \r\n"
				+ "BASIS_Duration_PY,BASIS_Duration_P2Y,BASIS_Duration_P3Y,Alpha,Beta,Typical,Beneficiary,CatLib,DuetoVolume,ReportedSpend,ModeledSpend, \r\n"
				+ "Channel,ChannelVolume,AllOutletVolume,ProjectionFactor,rNR,rNCS,rCtb,rAC,Volume,AC,Cont,Xnorm1,Xnorm2,Xnorm3,X1,X1_2,X1_3,X2,Gamma_X1,Gamma_X1_2,Gamma_X1_3,Gamma_X2 from vehicle_gamma UNION ALL \r\n"
				+ "select Level,Country,PeriodType,Report_Period,Actual_Period,SourceBDA,BrandChapter,Studio,Neighborhoods,BU,Division,SubChannel,MediaChannel, \r\n"  
				+"Market,ConsumerBehavior,EventName,'Yes' as Published,ReportedSpend as Spend,GRPs,Duration,Continuity,BASIS_PY,BASIS_P2Y,BASIS_P3Y, \r\n"  
				+"BASIS_Duration_PY,BASIS_Duration_P2Y,BASIS_Duration_P3Y,Alpha,Beta,Typical,Beneficiary,CatLib,DuetoVolume,ReportedSpend,ModeledSpend, \r\n"  
				+"Channel,ChannelVolume,AllOutletVolume,ProjectionFactor,rNR,rNCS,rCtb,rAC,Volume,AC,Cont,Xnorm1,Xnorm2,Xnorm3,X1,X1_2,X1_3,X2,Gamma_X1,Gamma_X1_2,Gamma_X1_3,Gamma_X2 from campaign_gamma "));
						


		// Push Final output to BigQuery
		PCollection<MyOutputClass> final_output = AppendQuery.apply(ParDo.of(new DoFn<BeamRecord, MyOutputClass>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				BeamRecord record = c.element();
				String strArr = record.toString();
				String strArr1 = strArr.substring(24);
				String xyz = strArr1.replace("]", "");
				String[] strArr2 = xyz.split(",");
				MyOutputClass moc = new MyOutputClass();
				moc.setLevel(strArr2[0]);
				moc.setCountry(strArr2[1]);
				moc.setPeriodType(strArr2[2]);
				moc.setReport_Period(strArr2[3]);
				moc.setActual_Period(strArr2[4]);
				moc.setSourceBDA(strArr2[5]);
				moc.setBrandChapter(strArr2[6]);
				moc.setStudio(strArr2[7]);
				moc.setNeighborhoods(strArr2[8]);
				moc.setBU(strArr2[9]);
				moc.setDivision(strArr2[10]);
				moc.setSubChannel(strArr2[11]);
				moc.setMediaChannel(strArr2[12]);
				moc.setMarket(strArr2[13]);
				moc.setConsumerBehavior(strArr2[14]);
				moc.setEventName(strArr2[15]);
				moc.setPublished(strArr2[16]);
				moc.setSpend(Double.parseDouble(strArr2[17]));
				moc.setGRPs(Double.parseDouble(strArr2[18]));
				moc.setDuration(Double.parseDouble(strArr2[19]));
				moc.setContinuity(Double.parseDouble(strArr2[20]));
				moc.setBASIS_PY(Double.parseDouble(strArr2[21]));
				moc.setBASIS_P2Y(Double.parseDouble(strArr2[22]));
				moc.setBASIS_P3Y(Double.parseDouble(strArr2[23]));
				moc.setBASIS_Duration_PY(Double.parseDouble(strArr2[24]));
				moc.setBASIS_Duration_P2Y(Double.parseDouble(strArr2[25]));
				moc.setBASIS_Duration_P3Y(Double.parseDouble(strArr2[26]));
				moc.setAlpha(Double.parseDouble(strArr2[27]));
				moc.setBeta(Double.parseDouble(strArr2[28]));
				moc.setTypical(Double.parseDouble(strArr2[29]));
				moc.setBeneficiary(strArr2[30]);
				moc.setCatLib(strArr2[31]);
				moc.setDuetoVolume(Double.parseDouble(strArr2[32]));
				moc.setReportedSpend(Double.parseDouble(strArr2[33]));
				moc.setModeledSpend(Double.parseDouble(strArr2[34]));
				moc.setChannel(strArr2[35]);
				moc.setChannelVolume(Double.parseDouble(strArr2[36]));
				moc.setAllOutletVolume(Double.parseDouble(strArr2[37]));
				moc.setProjectionFactor(Double.parseDouble(strArr2[38]));
				moc.setrNR(Double.parseDouble(strArr2[39]));
				moc.setrNCS(Double.parseDouble(strArr2[40]));
				moc.setrCtb(Double.parseDouble(strArr2[41]));
				moc.setrAC(Double.parseDouble(strArr2[42]));
				moc.setVolume(Double.parseDouble(strArr2[43]));
				moc.setAC(Double.parseDouble(strArr2[44]));
				moc.setCont(Double.parseDouble(strArr2[45]));
				moc.setXnorm1(Double.parseDouble(strArr2[46]));
				moc.setXnorm2(Double.parseDouble(strArr2[47]));
				moc.setXnorm3(Double.parseDouble(strArr2[48]));
				moc.setX1(Double.parseDouble(strArr2[49]));
				moc.setX1_2(Double.parseDouble(strArr2[50]));
				moc.setX1_3(Double.parseDouble(strArr2[51]));
				moc.setX2(Double.parseDouble(strArr2[52]));
				moc.setGamma_X1(Double.parseDouble(strArr2[53]));
				moc.setGamma_X1_2(Double.parseDouble(strArr2[54]));
				moc.setGamma_X1_3(Double.parseDouble(strArr2[55]));
				moc.setGamma_X2(Double.parseDouble(strArr2[56]));		
				c.output(moc);
			}
		}));
		TableSchema tableSchema = new TableSchema().setFields(
				ImmutableList.of(new TableFieldSchema().setName("Level").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Country").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("PeriodType").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Report_Period").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Actual_Period").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("SourceBDA").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("BrandChapter").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Studio").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Neighborhoods").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("BU").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Division").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("SubChannel").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("MediaChannel").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Market").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("ConsumerBehavior").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("EventName").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Published").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("Spend").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("GRPs").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Duration").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Continuity").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_PY").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_P2Y").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_P3Y").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_Duration_PY").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_Duration_P2Y").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("BASIS_Duration_P3Y").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Alpha").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Beta").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Typical").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Beneficiary").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("CatLib").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("DuetoVolume").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("ReportedSpend").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("ModeledSpend").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Channel").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("ChannelVolume").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("AllOutletVolume").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("ProjectionFactor").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("rNR").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("rNCS").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("rCtb").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("rAC").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Volume").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("AC").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Cont").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Xnorm1").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Xnorm2").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Xnorm3").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("X1").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("X1_2").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("X1_3").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("X2").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Gamma_X1").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Gamma_X1_2").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Gamma_X1_3").setType("FLOAT64").setMode("NULLABLE"),
						new TableFieldSchema().setName("Gamma_X2").setType("FLOAT64").setMode("NULLABLE")));
		TableReference tableSpec = BigQueryHelpers.parseTableSpec("ad-efficiency-dev:Ad_Efficiency.AdTest"); // project_id:Dataset.table
		System.out.println("Start Bigquery");
		final_output
				.apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via((MyOutputClass elem) -> new TableRow()
						.set("Level", elem.Level).set("Country", elem.Country).set("PeriodType", elem.PeriodType)
						.set("Report_Period", elem.Report_Period).set("Actual_Period", elem.Actual_Period)
						.set("SourceBDA", elem.SourceBDA).set("BrandChapter", elem.BrandChapter)
						.set("Studio", elem.Studio).set("Neighborhoods", elem.Neighborhoods).set("BU", elem.BU)
						.set("Division", elem.Division).set("SubChannel", elem.SubChannel)
						.set("MediaChannel", elem.MediaChannel).set("Market", elem.Market)
						.set("ConsumerBehavior", elem.ConsumerBehavior).set("EventName", elem.EventName)
						.set("Published", elem.Published).set("Spend", elem.Spend).set("GRPs", elem.GRPs).set("Duration",elem.Duration)
		                .set("Continuity", elem.Continuity).set("BASIS_PY", elem.BASIS_PY).set("BASIS_P2Y", elem.BASIS_P2Y)
						.set("BASIS_P3Y", elem.BASIS_P3Y).set("BASIS_Duration_PY", elem.BASIS_Duration_PY).set("BASIS_Duration_P2Y", elem.BASIS_Duration_P2Y).set("BASIS_Duration_P3Y", elem.BASIS_Duration_P3Y)
						.set("Alpha", elem.Alpha).set("Beta", elem.Beta).set("Typical", elem.Typical)
						.set("Beneficiary", elem.Beneficiary).set("CatLib", elem.CatLib).set("DuetoVolume", elem.DuetoVolume)
						.set("ReportedSpend", elem.ReportedSpend).set("ModeledSpend", elem.ModeledSpend).set("Channel", elem.Channel)
						.set("ChannelVolume", elem.ChannelVolume).set("AllOutletVolume", elem.AllOutletVolume).set("ProjectionFactor", elem.ProjectionFactor).set("rNR", elem.rNR)
						.set("rNCS", elem.rNCS).set("rCtb", elem.rCtb).set("rAC", elem.rAC)
						.set("Volume", elem.Volume).set("AC", elem.AC).set("Cont", elem.Cont)
						.set("Xnorm1", elem.Xnorm1).set("Xnorm2", elem.Xnorm2).set("Xnorm3", elem.Xnorm3)
						.set("X1", elem.X1).set("X1_2", elem.X1_2)
						.set("X1_3", elem.X1_3).set("X2", elem.X2).set("Gamma_X1", elem.Gamma_X1)
						.set("Gamma_X1_2", elem.Gamma_X1_2).set("Gamma_X1_3", elem.Gamma_X1_3).set("Gamma_X2", elem.Gamma_X2)))
				.apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(tableSchema)
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));   

		p.run().waitUntilFinish();
	}
}

// Some kind of rough work....
/*
 * com.google.cloud.storage.Storage storage =
 * StorageOptions.getDefaultInstance().getService(); BlobId b =
 * BlobId.of("cloroxtegbucket", "wcd.txt"); boolean deleted = storage.delete(b);
 * 
 * public void abcd(String bucket, String blobname) {
 * 
 * com.google.cloud.storage.Storage storage =
 * StorageOptions.getDefaultInstance().getService(); BlobInfo blob =
 * BlobInfo.newBuilder(bucket, blobname).build(); com.google.cloud.storage.Blob
 * remoteBlob = storage.create(blob); assertNotNull(remoteBlob); BlobId
 * wrongGenerationBlob = BlobId.of(bucket, blobname);
 * assertNull(storage.get(wrongGenerationBlob));
 * assertTrue(remoteBlob.delete()); }
 * 
 */
// Movng a file from one location to other

// void deleteObject("/input/Weekly_ip/abc.txt","cloroxtegbucket");
/*
 * Storage client = StorageFactory.getService();
 * client.objects().delete("cloroxtegbucket",
 * "/input/Weekly_ip/abc.txt").execute();
 */ /*
	 * com.google.cloud.storage.Storage storage =
	 * StorageOptions.getDefaultInstance().getService(); BlobId blobId =
	 * BlobId.of("cloroxtegbucket", "input_temp/abc.txt"); BlobInfo blobInfo =
	 * BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	 * com.google.cloud.storage.Blob blob = storage.create(blobInfo); // CopyWriter
	 * copyWriter = blob.copyTo(BlobId.of("cloroxtegbucket",
	 * "/input/Weekly_ip/abc.txt")); // Blob copiedBlob = (Blob)
	 * copyWriter.getResult(); boolean deleted = blob.delete();
	 */
