import java.sql.*;

public class TestConnection {
    public static void main(String[] args) throws Exception {
        // Charger explicitement le driver Oracle
        Class.forName("oracle.jdbc.OracleDriver");

        // Remplace par l'IP ou hostname correct du container Oracle
        String url = "jdbc:oracle:thin:@localhost:1521/FREEPDB1";  
        String user = "C##DBZUSER";      // utilisateur créé
        String password = "dbz";          // mot de passe

        Connection conn = DriverManager.getConnection(url, user, password);
        System.out.println("Connected!");
        conn.close();
    }
}