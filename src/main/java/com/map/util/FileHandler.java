package com.map.util;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class FileHandler {

	public FileHandler() {
	}

	// 创建目录
	public static void mkdir(String path) {
		File f = new File(path);
		if (!f.exists()) {
			f.mkdir();
		}
	}

	public static void mkdirs(String path) {
		File f = new File(path);
		if (!f.exists()) {
			f.mkdirs();
		}
	}
	
	public static boolean exist(String file) {
		File f = new File(file);
		if (f.exists()) {
			return true;
		} else {
			return false;
		}
	}

	public static void deleteFile(String file) {
		File f = new File(file);
		if (f.exists()) {
			f.delete();
		}
	}
	public static void deleteFolder(String folder){
		deleteAll(new File(folder));
	}
	public static void deleteAll(File path){ 
		if(!path.exists()){
			return; 
		}
		if(path.isFile()){ 
			path.delete(); 
			return; 
		} 
		File[] files = path.listFiles();     
	    for(int i=0;i<files.length;i++){ 
	    	deleteAll(files[i]); 
	    } 
		path.delete(); 
	}
	
	public static void copyFile(String src, String dest) {
		File d = new File(dest);
		if (d.exists()) {
			d.delete();
		}
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(src), "UTF-8"));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(dest), "UTF-8"));
			String tmp = br.readLine();
			if (tmp != null) {
				bw.write(tmp);
			}
			while ((tmp = br.readLine()) != null) {
				bw.newLine();
				bw.write(tmp);
			}
			br.close();
			bw.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static String readFile(String filePathName) {
		StringBuffer sb = new StringBuffer();	
		try {			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePathName), "UTF-8"));
			int ch;
			while ((ch = br.read()) != -1) {
				sb.append((char) ch);
			}
			br.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
	public static String readFile(String filePathName,String charSet) {
		StringBuffer sb = new StringBuffer();	
		try {			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePathName), charSet));
			int ch;
			while ((ch = br.read()) != -1) {
				sb.append((char) ch);
			}
			br.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return sb.toString();
	}
	public static void writeFile(String filePathName, String content, boolean append) {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filePathName,append), "GB18030"));
			bw.write(content);
			bw.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	public static void writeFile(String filePathName, String content,String charset) {
		try {
			File f = new File(filePathName);
			if (f.exists()) {
				f.delete();
			}
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filePathName), charset));
			bw.write(content);
			bw.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	public static void copyBinaryFile(File source, String dest) {
		try {;
			File out = new File(dest);
			FileInputStream inFile = new FileInputStream(source);
			FileOutputStream outFile = new FileOutputStream(out);
			byte[] buffer = new byte[1024];
			int i = 0;
			while ((i = inFile.read(buffer)) != -1) {
				outFile.write(buffer, 0, i);
			}// end while
			inFile.close();
			outFile.close();
		}// end try
		catch (Exception e) {
			e.printStackTrace();
		}// end catch
	}// end copyFile
	public static void copyBinaryFile(String source, String dest) {
		try {
			File in = new File(source);
			File out = new File(dest);
			FileInputStream inFile = new FileInputStream(in);
			FileOutputStream outFile = new FileOutputStream(out);
			byte[] buffer = new byte[1024];
			int i = 0;
			while ((i = inFile.read(buffer)) != -1) {
				outFile.write(buffer, 0, i);
			}// end while
			inFile.close();
			outFile.close();
		}// end try
		catch (Exception e) {
			e.printStackTrace();
		}// end catch
	}// end copyFile

	// 处理目录
	public static void copyFolder(String source, String dest) {
		String source1;
		String dest1;

		File[] file = (new File(source)).listFiles();
		for (int i = 0; i < file.length; i++)
			if (file[i].isFile()) {
				source1 = source + "/" + file[i].getName();
				dest1 = dest + "/" + file[i].getName();
				copyBinaryFile(source1, dest1);
			}// end if
		for (int i = 0; i < file.length; i++)
			if (file[i].isDirectory()) {
				source1 = source + "/" + file[i].getName();
				dest1 = dest + "/" + file[i].getName();
				File dest2 = new File(dest1);
				dest2.mkdir();
				copyFolder(source1, dest1);
			}// end if
	}// end copyDict

	/**
	 * 解压ZIP文件
	 * 
	 * @param zipFile
	 *            要解压的ZIP文件路径
	 * @param destination
	 *            解压到哪里
	 * @throws IOException
	 */
	public static void decompression(String zipFile, String destination) {
		try {
			ZipFile zip = new ZipFile(zipFile);
			Enumeration en = zip.entries();
			ZipEntry entry = null;
			byte[] buffer = new byte[8192];
			int length = -1;
			InputStream input = null;
			BufferedOutputStream bos = null;
			File file = null;

			while (en.hasMoreElements()) {
				entry = (ZipEntry) en.nextElement();
				if (entry.isDirectory()) {
					System.out.println("directory");
					continue;
				}

				input = zip.getInputStream(entry);
				file = new File(destination, entry.getName());
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				bos = new BufferedOutputStream(new FileOutputStream(file));

				while (true) {
					length = input.read(buffer);
					if (length == -1)
						break;
					bos.write(buffer, 0, length);
				}
				bos.close();
				input.close();
			}
			zip.close();
		} catch (Exception e) {
			// TODO 自动生成 catch 块
			e.printStackTrace();
		}
	}
	public static void readFileFromFolder(File file,ArrayList files){		
		if(file.isFile()){
			files.add(file);
		}else{
			File[] fs=file.listFiles();
			for(int i=0;i<fs.length;i++){
				if(fs[i].isFile()){
					files.add(fs[i]);
				}else{
					readFileFromFolder(fs[i],files);
				}
			}
		}
	}
	public static void downloadFile(String url,String filePathName) throws Exception{
		for (int i = 0; i < 10; i++) {
			try {
				URL net = new URL(url);
				InputStream in = null;
				HttpURLConnection conn = (HttpURLConnection) net.openConnection();
				conn.setConnectTimeout(10*1000);
				conn.setReadTimeout(10*1000);
				in = conn.getInputStream();
				BufferedInputStream bis = new BufferedInputStream(in);
				FileOutputStream fos = new FileOutputStream(new File(filePathName));
				int ch;
				while ((ch = bis.read()) != -1) {
					fos.write(ch);
				}		
				in.close();
				fos.close();
				break;
			} catch (Exception e) {
				// TODO 自动生成 catch 块
				if (i == 9) {
					break;
				}
				continue;
			}
		}
	}
	public static void writeSeriaObject(Object o,String filePathName) throws Exception{
		ObjectOutputStream oos=new ObjectOutputStream(new FileOutputStream(filePathName));
		oos.writeObject(o);
		oos.close();
	}
	public static Object readSeriaObject(String filePathName) throws Exception{
		ObjectInputStream ois=new ObjectInputStream(new FileInputStream(filePathName));
		return ois.readObject();
	}
	public static void writeExcelFile(String filePathName,String templatePath, 
			List<String[]> list, int sheetIndex) throws Exception {
		try {
			File file = new File(templatePath);
			InputStream in = new FileInputStream(file);
			HSSFWorkbook wk = new HSSFWorkbook(in);
			HSSFSheet sheet = wk.getSheetAt(sheetIndex);

			HSSFRow row = null;
			int lrn = sheet.getLastRowNum();
			if (lrn == 0)
				lrn = 1;
			for (int i = 0; i < list.size(); i++) {
				String[] values = list.get(i);
				row = sheet.createRow(i + lrn);
				HSSFCell cell = null;
				for (int j = 0; j < values.length; j++) {
					cell = row.createCell(j);
					cell.setCellType(HSSFCell.CELL_TYPE_STRING);
					String value = values[j];
					cell.setCellValue(value);
				}
			}

			File outfile = new File(filePathName);
			OutputStream out = new FileOutputStream(outfile);
			wk.write(out);
			wk.close();
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	 // headerIndex为表头索引,0表示第一行
	public static List<Map<String, Object>> readExcel(String fileName, 
			int sheetIndex, int headerIndex) throws IOException {
		HSSFWorkbook wb = null;
		List<Map<String, Object>> dataList = new ArrayList<Map<String,Object>>();
		try {
			InputStream is = new FileInputStream(fileName);
			POIFSFileSystem fs = new POIFSFileSystem(is);
			wb = new HSSFWorkbook(fs);
			HSSFSheet sheet = wb.getSheetAt(sheetIndex);
			HSSFRow header = null;	// header
			for (int i=0; i<= headerIndex; i++) {
				HSSFRow row = sheet.getRow(i);
				sheet.removeRow(row);		
				if (i == headerIndex){
					header = row;
				}
			}
			
			Iterator<Row> rowIter = sheet.rowIterator();
			while ( rowIter.hasNext() ) {
				Row row = rowIter.next();
				Map<String, Object> m = new HashMap<String, Object>();
				Iterator<Cell> hIter = header.cellIterator();
				int rowNum = row.getRowNum();
				m.put("ROWNUM", rowNum);
				while ( hIter.hasNext() ) {
					Cell hCell = hIter.next();
					int hIndex = hCell.getColumnIndex();
					String hName = hCell.getStringCellValue();
					
					Object value = "";
					Cell cell = row.getCell(hIndex);
					if ( cell != null ) {
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_NUMERIC:
							cell.setCellType(Cell.CELL_TYPE_STRING);
							value = cell.getStringCellValue();
							break;
						case Cell.CELL_TYPE_BOOLEAN:
							value = cell.getBooleanCellValue();
							break;
						case Cell.CELL_TYPE_ERROR:
						case Cell.CELL_TYPE_FORMULA:
						case Cell.CELL_TYPE_BLANK:
						case Cell.CELL_TYPE_STRING:
							value = cell.getStringCellValue();
							break;
						}
					}
					m.put(hName.toUpperCase(), value);
				}
				dataList.add(m);
			}
			wb.close();
		} catch (IOException e) {
			System.err.println("Read "+ fileName +" error!"+e);
		} finally {
			if(wb!=null)
				try {
					wb.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return dataList;
	}
	public static BufferedWriter getWriter(String outfile,String charset)
			throws FileNotFoundException, UnsupportedEncodingException {
		File file = new File(outfile);
		OutputStream stream = new FileOutputStream(file, true);
		Writer writer = new OutputStreamWriter(stream, charset);
		BufferedWriter bfWriter = new BufferedWriter(writer);
		return bfWriter;
	}

	public static BufferedReader getReader(String path,String charset)
			throws FileNotFoundException, UnsupportedEncodingException {
		File file = new File(path);
		InputStream stream = new FileInputStream(file);
		Reader reader = new InputStreamReader(stream, charset);
		BufferedReader bfReader = new BufferedReader(reader);
		return bfReader;
	}
	
	public static void main(String[] args) {
		FileHandler.writeFile("C:/Users/shenhuajin/Desktop/sssssss.txt", "22", true);
	}
//class end
}
