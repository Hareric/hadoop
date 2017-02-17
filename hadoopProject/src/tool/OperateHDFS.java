package tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
 
public class OperateHDFS {
 
    /**
     * 加载配置文件
     */
    static Configuration conf = new Configuration();
 
    /**
     * 读取HDFS某个文件夹的所有文件，并打印
     * 
     * @param hdfsPath
     *            读取HDFS哪个目录下的文件，HDFS目录
     * @throws IOException
     */
    public static void readHDFSAll(String hdfsPath) throws IOException {
        InputStream in = null;
 
        // 读取HDFS上的文件系统，获取到的是一个org.apache.hadoop.hdfs.DistributedFileSystem对象
        FileSystem hdfs = FileSystem.get(URI.create(hdfsPath), conf);
 
        // 使用缓冲流，进行按行读取的功能
        BufferedReader buff = null;
 
        // 获取要读取的文件的根目录
        Path listFiles = new Path(hdfsPath);
 
        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus stats[] = hdfs.listStatus(listFiles);
 
        for (int i = 0; i < stats.length; i++) {
            // 判断是不是目录，如果是目录，递归调用
            if (stats[i].isDirectory()) {
                readHDFSAll(stats[i].getPath().toString());
            }
            else {
                System.out.println("文件路径名:" + stats[i].getPath().toString());
                // 获取Path
                Path p = new Path(stats[i].getPath().toString());
                // 打开文件流
                in = hdfs.open(p);
                buff = new BufferedReader(new InputStreamReader(in));
                String str = null;
                while ((str = buff.readLine()) != null) {
                    System.out.println(str);
                }
                buff.close();
                in.close();
            }
        }
    }
 
    /**
     * 重命名HDFS上的文件夹或者文件
     * 
     * @param srcPath
     *            要修改的HDFS的文件或者文件夹，HDFS目录
     * @param destPath
     *            修改成什么文件或者文件夹，HDFS目录
     * @throws IOException
     */
    public static void renameFileOrDirectoryOnHDFS(String srcPath,
            String destPath) throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(srcPath), conf);
 
        Path src = new Path(srcPath);
        Path dest = new Path(destPath);
 
        hdfs.rename(src, dest);
        // 释放资源
        hdfs.close();
 
        System.out.println("重命名成功");
 
    }
 
    /**
     * 从HDFS下载文件或者文件夹的所有内容到本地
     * 
     * @param downloadPath
     *            下载哪个文件，或者哪个目录下的所有文件，HDFS目录
     * @param localPath
     *            下载文件到哪里，本地目录
     * @throws IOException
     */
    public static void downloadFileorDirectoryOnHDFS(String downloadPath,
            String localPath) throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(downloadPath), conf);
 
        // 要从哪里下载
        Path download = new Path(downloadPath);
        // 下载到本地的哪个目录
        Path localDir = new Path(localPath);
 
        // 从HDFS拷贝到本地目录下
        hdfs.copyToLocalFile(download, localDir);
        // 释放资源
        hdfs.close();
 
        System.out.println("下载成功");
    }
 
    /**
     * 从本地上传文件或者文件夹到HDFS
     * 
     * @param uploadPath
     *            上传哪个目录下的文件或者文件夹，本地目录
     * @param localPath
     *            上传文件到哪里，HDFS目录
     * @throws Exception
     */
    public static void uploadFileorDirectoryToHDFS(String uploadPath,
            String destPath) throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(destPath), conf);
 
        // 本地文件或者目录
        Path localDir = new Path(uploadPath);
 
        // 要上传到哪里
        Path dest = new Path(destPath);
 
        // 从本地目录拷贝到HDFS目录下，没有目录，会自动创建目录
        hdfs.copyFromLocalFile(localDir, dest);
        // 释放资源
        hdfs.close();
 
        System.out.println("上传成功");
    }
 
    /**
     * 在HDFS上创建文件夹
     * 
     * @param pathName
     *            文件夹的路径，HDFS目录
     * @throws IOException
     */
    public static void createDirectoryOnHDFS(String pathName)
            throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(pathName), conf);
        Path newPath = new Path(pathName);
        // 在HDFS上创建文件夹
        hdfs.mkdirs(newPath);
        // 释放资源
        hdfs.close();
        System.out.println("创建文件夹成功");
    }
 
    /**
     * 在HDFS上删除文件夹
     * 
     * @param pathName
     *            文件夹的路径，HDFS目录
     * @throws IOException
     */
    public static void deleteDirectoryOnHDFS(String deletePath)
            throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(deletePath), conf);
        Path deleteDir = new Path(deletePath);
        // 在HDFS上删除文件夹
        hdfs.deleteOnExit(deleteDir);
        // 释放资源
        hdfs.close();
        System.out.println("删除文件夹成功");
    }
 
    /**
     * 在HDFS上创建文件
     * 
     * @param filePath
     *            创建的文件的名称，HDFS目录
     * @throws IOException
     */
    public static void createFileOnHDFS(String filePath) throws IOException {
        FileSystem hdfs = FileSystem.get(URI.create(filePath), conf);
        Path newFile = new Path(filePath);
        hdfs.createNewFile(newFile);
        // 释放资源
        hdfs.close();
        System.out.println("创建文件成功");
    }
 
    /**
     * 在HDFS上删除文件
     * 
     * @param filePath
     *            删除的文件的名称，HDFS目录
     * @throws IOException
     */
    public static void deleteFileOnHDFS(String filePath) throws IOException {
        FileSystem hdfs = FileSystem.get(URI.create(filePath), conf);
        Path deleteFile = new Path(filePath);
        hdfs.deleteOnExit(deleteFile);
        hdfs.close();
        System.out.println("删除文件成功");
    }
 
    public static void main(String[] args) throws IOException {
 
        // 读取一个目录下面的所有的文件包括子目录下的文件
//        readHDFSAll("hdfs://192.168.3.57:8020/user/lxy/input");
    	readHDFSAll("weibo");
 
//        // 重命名HDFS上的文件
//        renameFileOrDirectoryOnHDFS(
//                "hdfs://192.168.3.57:8020/user/lxy/mergeresult/merge.txt",
//                "hdfs://192.168.3.57:8020/user/lxy/mergeresult/renamemerge.txt");
// 
//        // 重命名HDFS上的文件夹
//        renameFileOrDirectoryOnHDFS(
//                "hdfs://192.168.3.57:8020/user/lxy/mergeresult",
//                "hdfs://192.168.3.57:8020/user/lxy/rename");
// 
        // 从HDFS下载文件或者文件夹
//        downloadFileorDirectoryOnHDFS(
//        		"hdfs://localhost:9000/output",
//                "/Users/Har/Desktop");
//        // 从本地上传文件到HDFS
//        uploadFileorDirectoryToHDFS("E:\\test\\upload\\lib",
//                "hdfs://192.168.3.57:8020/user/lxy");
// 
//        // 在HDFS上创建文件夹createDir
//        createDirectoryOnHDFS("hdfs://192.168.3.57:8020/user/lxy/createDir");
// 
//        // 在HDFS上删除文件夹createDir
//        deleteDirectoryOnHDFS("hdfs://192.168.3.57:8020/user/lxy/createDir");
// 
//        // 在HDFS上创建文件newFile.txt
//        createFileOnHDFS("hdfs://192.168.3.57:8020/user/lxy/newFile.txt");
// 
//        // 在HDFS上删除文件newFile.txt
//        deleteFileOnHDFS("hdfs://192.168.3.57:8020/user/lxy/newFile.txt");
    }
    
}
