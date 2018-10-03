package my.proj;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.beam.sdk.repackaged.org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;

public class test {
	
	public static class Test1{
        static void convert(File inputFile, File outputFile) {

     try {
         FileOutputStream fos = new FileOutputStream(outputFile);
         // Get the workbook object for XLSX file
         Workbook wBook = WorkbookFactory.create(inputFile);
//         XSSFWorkbook wBook = new XSSFWorkbook(
//                 new FileInputStream(inputFile));
         StringBuffer data = new StringBuffer();
         // Get first sheet from the workbook
         Sheet sheet = wBook.getSheetAt(0);
         Row row;
         Cell cell;
         // Iterate through each rows from first sheet
         Iterator<Row> rowIterator = sheet.iterator();

         while (rowIterator.hasNext()) {
             row = rowIterator.next();

             // For each row, iterate through each columns
             Iterator<Cell> cellIterator = row.cellIterator();
             while (cellIterator.hasNext()) {

                 cell = cellIterator.next();

					if (cell.getCellType() == cell.CELL_TYPE_FORMULA) {
						// Checking the cell format
						switch (cell.getCachedFormulaResultType()) {
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
					} else {
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
             data.append("\n");
         }

         fos.write(data.toString().getBytes());
         fos.close();

     } catch (Exception ioe) {
         ioe.printStackTrace();
     }
 }

 // testing the application

	public static void main(String[] args) throws InvalidFormatException, EncryptedDocumentException, org.apache.poi.openxml4j.exceptions.InvalidFormatException, IOException {
		File input = new File("C:\\Users\\uday.kumar\\Downloads\\HVR_FY18Q4_Spend.xlsx");
		File output = new File("C:\\Users\\uday.kumar\\Downloads\\HVR_FY18Q4_Spend.csv");
		
		convert(input,output);
		
	}
	}
}
