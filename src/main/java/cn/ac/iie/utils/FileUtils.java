package cn.ac.iie.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * 文件操作相关工具类
 *
 * @author Xiang
 * @date 2016-09-25 14:25
 */
public class FileUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 若目录不存在时创建目录
     * @param dir 目录文件对象
     * @return 返回创建结果
     * 若文件存在且文件为目录则返回true；<br/>
     * 若文件不存在则递归创建目录且返回创建结果；<br/>
     * 否则，返回false。
     */
    public static boolean createDirIfNotExists(File dir) {
        boolean exists = dir.exists();
        return exists && dir.isDirectory() || !exists && dir.mkdirs();
    }

    /**
     * 判断文件是否被其它进程占用<br/>
     * 判断原理为：<br/>
     * 对文件上锁，如果能够成功上锁，则说明文件不被其它进程占用；<br/>
     * 否则，说明文件被其它进程占用。
     * @param f 文件对象
     * @return 如果文件不存在或文件不被其它进程占用，返回true；<br/>
     * 否则，返回false。
     */
    public static boolean noOthersUsing(File f){
        boolean noUsing = false;
        FileLock lock = null;
        RandomAccessFile raf=null;
        FileChannel channel=null;
        try {
            raf = new RandomAccessFile(f, "rw");
            channel = raf.getChannel();
            lock = channel.tryLock();
            noUsing = lock.isValid();
        } catch (FileNotFoundException e) {
            noUsing = true;
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (lock != null) {
                try {
                    lock.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
           if(channel!=null){
               try {
                   channel.close();
               } catch (IOException e) {
               }
           }
           if(raf!=null){
               try {
                   raf.close();
               } catch (IOException e) {
               }
           }
        }
        return noUsing;
    }

    /**
     * 判断两个文件大小是否相同
     * @param f 文件1
     * @param l 文件2
     * @return 返回两个文件大小是否相同
     */
    public static boolean isSameLength(File f, File l){
        return f.length() == l.length();
    }

    /**
     * 删除指定文件
     * @param file 指定待删除文件
     * @return 返回是否删除成功
     */
    public static boolean deleteFile(File file) {
        try {
            return Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 迁移文件
     * @param sourcePath 源文件路径
     * @param targetPath 目标文件路径
     */
    public static boolean copyFile(Path sourcePath, Path targetPath){
        boolean copied = false;
        try {
            // 将源文件移动至目标文件，同时需要拷贝文件属性
            Files.copy(sourcePath, targetPath, REPLACE_EXISTING, COPY_ATTRIBUTES);
            LOG.info("{} has been copied to {}", sourcePath.toString(), targetPath.toString());
            copied = true;
        } catch (IOException e) {
            LOG.error("IOException encountered during file copy, src:{},dst:{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Exception encountered during file copy, src:{},dst:{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        }
        return copied;
    }

     public static boolean copyFileLessInfo(Path sourcePath, Path targetPath){
        boolean copied = false;
        try {
            // 将源文件移动至目标文件，同时需要拷贝文件属性
            Files.copy(sourcePath, targetPath, REPLACE_EXISTING, COPY_ATTRIBUTES);
            copied = true;
        } catch (IOException e) {
            LOG.error("IOException encountered during file copy, src:{},dst:{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Exception encountered during file copy, src:{},dst:{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        }
        return copied;
    }

    /**
     * move file or directory
     * @param sourcePath
     * @param targetPath
     * @return
     */
    public static boolean moveFile(Path sourcePath,Path targetPath){
        boolean moved=false;
        try{
            Files.move(sourcePath,targetPath,REPLACE_EXISTING);
            moved=true;
        }catch (IOException ex){
            LOG.error("IOException encountered during moving,src:{} dst:{}",sourcePath.toString(),targetPath.toString());
            LOG.error(ex.getMessage(),ex);
        }catch (Exception e){
            LOG.error("Exception encountered during moving,src:{} dst:{}",sourcePath.toString(),targetPath.toString());
            LOG.error(e.getMessage(),e);
        }
        return moved;
    }

    /**
     * 创建软连接
     * @param linkPath 连接文件路径
     * @param targetPath 目标文件路径
     * @return 返回是否创建成功
     * 当出现IO异常或其它异常时将会创建失败
     */
    public static boolean createSymbolicLink(Path linkPath, Path targetPath){
        boolean created = true;
        try {
            Files.createSymbolicLink(
                    linkPath,
                    targetPath);
            LOG.info("Symbol link created, src:{}，dst:{}", linkPath.toString(), targetPath.toString());
        } catch (IOException e) {
            LOG.error("IOException encountered when creating symbol line, src:{} dst:{}", linkPath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
            created = false;
        } catch (Exception e){
            LOG.error("Exception encountered when creating symbol line, src:{} dst:{}", linkPath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
            created = false;

        }
        return created;
    }

    public static boolean deleteDirecotry(File file){
        if(file.exists()==false){
            return false;
        }
        if(file.isDirectory()){
            File [] files=file.listFiles();
            for(File f:files){
                deleteDirecotry(f);
            }
            deleteFile(file);
        }else{
           deleteFile(file);
        }
        return true;
    }
}
