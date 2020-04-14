package simpledb;

import org.junit.jupiter.api.Test;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

public class Assignment1Test {

    @Test
    public void test_task_1() {

        SimpleDB.init("studentdb");
        FileMgr fm = SimpleDB.fileMgr();
        int filesize = fm.size("junk");
        Block blk = new Block("junk", filesize - 1);

        Page p1 = new Page();
        p1.read(blk);
        int n = p1.getInt(3992);
        p1.setInt(3992, n + 1);
        p1.write(blk);

        Page p2 = new Page();
        p2.setString(20, "hello");
        blk = p2.append("junk");

        Page p3 = new Page();
        p3.read(blk);
        String s = p3.getString(20);
        System.out.println("Block " + blk.number() + " contains " + s);

    }

}
