package cn.ac.iie.drive.commands.base;

/**
 * sstable-tool命令
 *
 * @author Xiang
 * @date 2016-09-22 11:03
 */
public abstract class SSTableToolCmd implements Runnable{
    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        if(validate()){
            execute();
        } else{
            System.out.println("命令参数不正确");
        }
    }

    protected boolean validate(){
        return false;
    }

    protected void execute(){
    }
}
