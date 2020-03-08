package elastic.mapper.importer;



import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

public final class FileUtil {
	/**
	 * 列出一个文件夹及其子文件夹下所有符合条件的文件名（包含绝对路径）
	 * @param path 文件夹路径
	 * @param suffix 文件扩展名
	 * @param ignoreCase 文件扩展名是否忽略大小写 true：忽略 false：不忽略
	 * @return
	 */
	public static List<String> listFileFullPath(String path,String suffix,boolean ignoreCase){
		if(!ignoreCase){//不忽略大小写
			return listFileFullPath(path,suffix);
		}
		
		suffix = suffix.toLowerCase();
		
		List<String> fullPaths = listFileFullPath(path,suffix);
		
		suffix = suffix.toUpperCase();
		
		List upperfullPaths = listFileFullPath(path,suffix);
		fullPaths.addAll(upperfullPaths);
		return fullPaths;
	}
	
	/**
	 * 列出一个文件夹及其子文件夹下所有符合条件的文件名（包含绝对路径）
	 * @param path  文件夹路径
	 * @param suffix  文件扩展名
	 * @return
	 */
	public static List<String> listFileFullPath(String path,String suffix){
		List fullPaths = new ArrayList<String>();
		File file = new File(path);
		Collection<File> coll =  FileUtils.listFiles(file,FileFilterUtils.suffixFileFilter(suffix),FileFilterUtils.directoryFileFilter());
		if(coll!=null&&coll.size()>0){
			Iterator<File> iter = coll.iterator();
			while(iter.hasNext()){
				file = iter.next();
				fullPaths.add(file.getAbsolutePath());
			}
		}
		
		return fullPaths;
	}
	
	public static void main(String[] args) {
		List<String> list = FileUtil.listFileFullPath("/home/gd/pdf","pdf",true);
		for(String str:list){
			System.out.println(str);
		}
	}
	
}