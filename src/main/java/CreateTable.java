import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Description TODO
 * author dragonKJ
 * createTime 2022/2/8  22:13
 */
public class CreateTable{

    public static void main(String[] args) {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3307/mytest?useUnicode=true&characterEncoding=utf8";
        String user = "root";
        String password = "root";
        Connection con = null;
        Statement pre = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            pre= con.createStatement();
            StringBuffer sql=new StringBuffer("create table air2022(date CHAR(8),type VARCHAR(20),site " +
                    "CHAR(5),num double ,primary key(date,type,site))");
            pre.execute(sql.toString());


        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            try {
                if (pre != null) pre.close();
                if (con != null) con.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
}
