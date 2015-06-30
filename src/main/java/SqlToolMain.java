import java.sql.*;
import java.util.ArrayList;

/**
 * Created by Oleg Tatarchuk on 23.06.2015.
 *
 * Before run please install 'Microsoft JDBC Driver 4.0 for SQL Server' from \lib\sqljdbc_4.0.2206.100_enu.exe
 *  and add dependencies
 *
 * Initial version 1.5
 *   + Main logic implemented.
 *   + Works only with 'nchar' and 'numeric' data types ('datetime' data type NOT implemented).
 *   + Code is NOT refactored, no OOP, just one page script.
 *   + Default, all numeric fields as ID - NOT implemented.
 *
 * Known bugs:
 *   + When rows in DB less then bulk size - exception.
 *
 *
 *
 */

public class SqlToolMain {
    public static void main (String[] args) {
        long startTime = System.currentTimeMillis();

        // User input
        // Auth and connection string
        String dbHostName            = "localhost";
        String dbServerPort          = "59593";
        String integratedSecurity    = "true";
        String userName              = "Oleg";
        String password              = "";
        String databaseName          = "SqlToolCustomers";
        String dbo                   = "dbo";
        // Settings
        String tableName             = "Customers";
        String considerThisFieldAsId = "CustomerId"; //(null - Default, all numeric fields (NOT implemented!))
        int bulkSize                 = 100;
        int iterations               = 100;


        try (
                Connection connection = DriverManager.getConnection(
                        //"jdbc:mysql://127.0.0.1:3306/sqltool_customers", "root", ""); // can be extended to use MySQL
                        "jdbc:sqlserver://"+dbHostName+":"+dbServerPort+";databaseName="+databaseName+";" +
                                "integratedSecurity="+integratedSecurity+";", userName, password); // SQLServer connection string
                Statement statement = connection.createStatement()
        ) {

// ******* DATA PREPARATION SECTION *************
            //Get info (columns, rows, data types...) from DB
            //Insert data preparation
            String strSelect = "SELECT TOP "+bulkSize+" * FROM "+dbo+"."+tableName+" ORDER BY "+considerThisFieldAsId+" DESC";
            //System.out.println("The SQL query is: " + strSelect); // Echo For debugging
            //System.out.println();
            ResultSet resultSet = statement.executeQuery(strSelect);
            ResultSetMetaData rsmd = resultSet.getMetaData();

            String columnName;
            int columnType;
            String columnData;
            int columnCount = rsmd.getColumnCount();
            String [][] dataSet = new String[bulkSize][columnCount]; //TODO: fix if rows in DB less then bulk size
            ArrayList<String>  columnNamesSet = new ArrayList();
            ArrayList<Integer> columnTypesSet = new ArrayList();
            int rowIterator = 1;
            while(resultSet.next()) {   // Move the cursor to the next row
                columnCount  = rsmd.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    columnName = rsmd.getColumnName(i);
                    columnType    = rsmd.getColumnType(i);
                    // verify column type and get data from it (getInt or getString)
                    if (columnType == 4) { // possible to extend this condition to more types
                        //System.out.println("columnType0: " + columnType); //DEBUG INFO
                        columnData = String.valueOf(resultSet.getInt(columnName));
                    } else {
                        //System.out.println("columnType1: " + columnType); //DEBUG INFO
                        columnData = resultSet.getString(columnName);
                    }
                    // Store column names one time
                    if (rowIterator == 1) {
                        columnNamesSet.add(columnName);
                        //System.out.print(columnName+"("+columnType+")" + ", ");
                        columnTypesSet.add(columnType);
                    }
                    //store data in matrix
                    dataSet[rowIterator-1][i-1] = columnData;
/*                  //DEBUG INFO
                    if (i == columnCount) {
                        System.out.println();
                    }*/
                }
/*              //DEBUG INFO
                System.out.println();
                for (int row = 0; row < dataSet.length; row++) {
                    for (int col = 0; col < dataSet[row].length; col++) {
                        System.out.print(dataSet[row][col]);
                        System.out.print(", ");
                    }
                }*/
                ++rowIterator;
            }

// ******* DATA INSERTION SECTION *************
            StringBuilder sqlInsertValuesString = new StringBuilder();
            int idValueInt = 0;
            //Iteration
            for (int i = 0; i < iterations; i++) {
                //Bulk
                for (int row = 0, iBulkSizeIterations = 1; row < dataSet.length; row++, iBulkSizeIterations++) {
                    sqlInsertValuesString.append("INSERT INTO "+tableName+" VALUES");
                    for (int col = 0; col < dataSet[row].length; col++) { //Bulk
                        if (col == 0) {
                            sqlInsertValuesString.append("(");
                        } else if (col > 0 && col <= dataSet[row].length-1) {
                            // verify data type
                            if (columnTypesSet.get(col) == 4) { //Numeric data type
                                if (columnNamesSet.get(col).equals(considerThisFieldAsId)) {
                                    if (row == 0 && i == 0) { //got initial value for increment from ID field
                                        idValueInt = Integer.valueOf(dataSet[row][col]);
                                    }
                                    idValueInt += 1;
                                    sqlInsertValuesString.append(idValueInt);
                                } else {
                                    sqlInsertValuesString.append(dataSet[row][col]);
                                }
                            } else if (columnTypesSet.get(col) == -15) { //nchar data type
                                sqlInsertValuesString.append("'");
                                sqlInsertValuesString.append(dataSet[row][col]);
                                sqlInsertValuesString.append("'");
                            } else { //else data types
                                sqlInsertValuesString.append("'");
                                sqlInsertValuesString.append(dataSet[row][col]);
                                sqlInsertValuesString.append("'");
                            }
                            if (col != dataSet[row].length-1) {
                                sqlInsertValuesString.append(",");
                            } else {
                                sqlInsertValuesString.append(")");
                            }
                        }
                        //DEBUG INFO
                        //System.out.print(dataSet[row][col]);
                        //System.out.print(", ");
                    }
                    //DEBUG INFO
                    //System.out.println();
                    //System.out.println();
                    //System.out.println("sqlInsertValuesString: "+sqlInsertValuesString);
                    statement.addBatch(sqlInsertValuesString.toString());
                    sqlInsertValuesString.setLength(0);
                }
            }

            int[] countInsertedSet = statement.executeBatch();
            int countInserted = 0;
            System.out.println();
            for (int ins : countInsertedSet) {
                countInserted++;
            }
            System.out.println("Rows inserted: "+countInserted);
            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println("Insert time (seconds): "+totalTime/1000);

            statement.close();
            connection.close();


        } catch(SQLException ex) {
            ex.printStackTrace();
        }
        // Close the resources - Done automatically by try-with-resources (closed earlier)
    }
}
