import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;


public class DBOutputWritable implements DBWritable {

    private String inputWords;
    private String followingWords;
    private int value;

    public DBOutputWritable(String inputWords, String followingWords, int value) {
        this.inputWords = inputWords;
        this.followingWords = followingWords;
        this.value = value;
    }

    public void readFields(ResultSet arg0) throws SQLException {
        inputWords = arg0.getString(1);
        followingWords = arg0.getString(2);
        value = arg0.getInt(3);
        return;
    }

    public void write(PreparedStatement arg0) throws SQLException {

        arg0.setString(1, inputWords);
        arg0.setString(2, followingWords);
        arg0.setInt(3, value);
        return;
    }

}
