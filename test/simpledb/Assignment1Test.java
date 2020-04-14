package simpledb;

import org.junit.jupiter.api.Test;
import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class Assignment1Test {

    // === Task 1 ===
    @Test
    public void given_page_size_with_400_bytes_when_setInt_with_offset_0_executed_then_it_saves_integer() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 396 (BLOCK_SIZE - INT_SIZE)
        int someIntegerValue = 123;
        int offset = 0;
        Page p1 = new Page();
        p1.read(blk);

        // When
        p1.setInt(offset, someIntegerValue);
        p1.write(blk);

        // Then
        assertEquals(123, p1.getInt(offset));
    }

    @Test
    public void given_page_size_with_400_bytes_when_setInt_with_offset_397_executed_then_it_throws_IllegalArgumentException() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 396 (BLOCK_SIZE - INT_SIZE)
        int someIntegerValue = 123;
        int offset = 397;
        Page p1 = new Page();
        p1.read(blk);

        // When
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> p1.setInt(offset, someIntegerValue));
        p1.write(blk);

        // Then
        assertEquals("Given value does not fit into buffer!", thrown.getMessage());
    }

    @Test
    public void given_page_size_with_400_bytes_and_example_string_with_4_bytes_when_setString_with_offset_0_executed_then_it_saves_string() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setString gives error after offset 392 (INT_SIZE(4bytes) + value(4bytes))
        String fourByteString = "1234";
        int offset = 0;
        Page p1 = new Page();
        p1.read(blk);

        // When
        p1.setString(offset, fourByteString);
        p1.write(blk);

        // Then
        assertEquals("1234", p1.getString(offset));
    }

    @Test
    public void given_page_size_with_400_bytes_and_example_string_with_4_bytes_when_setString_with_offset_393_executed_then_it_throws_IllegalArgumentException() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setString gives error after offset 392 (INT_SIZE(4bytes) + value(4bytes))
        String fourByteString = "1234";
        int offset = 393;
        Page p1 = new Page();
        p1.read(blk);

        // When
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> p1.setString(offset, fourByteString));
        p1.write(blk);

        // Then
        assertEquals("Given value does not fit into buffer!", thrown.getMessage());
    }

    // === Task 2 ===
    @Test
    public void given_page_size_with_400_bytes_when_setBoolean_with_offset_0_executed_then_it_saves_boolean() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 396 (BLOCK_SIZE - INT_SIZE)
        boolean someBooleanValue = true;
        int offset = 0;
        Page p1 = new Page();
        p1.read(blk);

        // When
        p1.setBoolean(offset, someBooleanValue);
        p1.write(blk);

        // Then
        assertTrue(p1.getBoolean(offset));
    }

    @Test
    public void given_page_size_with_400_bytes_when_setBoolean_with_offset_400_executed_then_it_throws_IllegalArgumentException() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 396 (BLOCK_SIZE - INT_SIZE)
        boolean someBooleanValue = true;
        int offset = 400;
        Page p1 = new Page();
        p1.read(blk);

        // When
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> p1.setBoolean(offset, someBooleanValue));
        p1.write(blk);

        // Then
        assertEquals("Given value does not fit into buffer!", thrown.getMessage());
    }

    @Test
    public void given_page_size_with_400_bytes_when_setDate_with_offset_0_executed_then_it_saves_date() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 396 (BLOCK_SIZE - INT_SIZE)
        Date someDateValue = new Date(1586876490);
        int offset = 0;
        Page p1 = new Page();
        p1.read(blk);

        // When
        p1.setDate(offset, someDateValue);
        p1.write(blk);

        // Then
        assertEquals(1586876490, p1.getDate(offset).getTime());
    }

    @Test
    public void given_page_size_with_400_bytes_when_setDate_with_offset_393_executed_then_it_throws_IllegalArgumentException() {
        // Given
        SimpleDB.init("studentdb");
        Block blk = new Block("junk", 0);

        // test if setInt gives error after offset 392 (BLOCK_SIZE - LONG_SIZE)
        Date someDateValue = new Date();
        int offset = 400;
        Page p1 = new Page();
        p1.read(blk);

        // When
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> p1.setDate(offset, someDateValue));
        p1.write(blk);

        // Then
        assertEquals("Given value does not fit into buffer!", thrown.getMessage());
    }

}
