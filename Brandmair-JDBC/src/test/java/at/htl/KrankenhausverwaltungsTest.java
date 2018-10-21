package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KrankenhausverwaltungsTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    private static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    static final String USER = "app";
    static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
            ddl();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Connection to the Database has failed: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void ddl() {
        try (Statement stmt = conn.createStatement()) {

            String doctorTableSql = "CREATE TABLE doctor (" +
                    "id INT CONSTRAINT doctors_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "occupation VARCHAR(255) NOT NULL," +
                    "salary DECIMAL(12,2) NOT NULL" +
                    ")";

            stmt.execute(doctorTableSql);

            String patientTableSql = "CREATE TABLE patient (" +
                    "id INT CONSTRAINT patient_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "salary DECIMAL(12,2) NOT NULL" + // Very questionable hospital practices
                    ")";

            stmt.execute(patientTableSql);

            String treatmentTableSql = "CREATE TABLE treatment (" +
                    "id INT CONSTRAINT treatment_pk PRIMARY KEY," +
                    "name VARCHAR(255) NOT NULL," +
                    "doctor_fk INT NOT NULL," +
                    "patient_fk INT NOT NULL," +
                    "outcome VARCHAR(255)," +
                    "CONSTRAINT treatment_fk_doctor FOREIGN KEY(doctor_fk) REFERENCES doctor(id)," +
                    "CONSTRAINT treatment_fk_patient FOREIGN KEY(patient_fk) REFERENCES patient(id)" +
                    ")";

            stmt.execute(treatmentTableSql);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc() {
        try (Statement stmt = conn.createStatement()) {
            String cleanupTreatmentTable = "drop table treatment";
            stmt.execute(cleanupTreatmentTable);

            String cleanupDoctorTable = "drop table doctor";
            stmt.execute(cleanupDoctorTable);

            String cleanupPatientTable = "drop table patient";
            stmt.execute(cleanupPatientTable);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test00_insertData() {
        // Insert doctors
        int numberOfInserts = 0;
        try (Statement stmt = conn.createStatement()) {
            String sql = "INSERT INTO doctor (id, name, occupation, salary) values(1, 'Dr. Stephen Strange', 'Surgeon', 10000000.02)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO doctor (id, name, occupation, salary) values(2, 'Dr. Wilhelm Mayor', 'Cardiologist', 300000.32)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO doctor (id, name, occupation, salary) values(3, 'Dr. Casey Washington', 'Veterinary physician', 60000.62)"; //Talk about questionable practices
            numberOfInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(numberOfInserts, is(3));

        // Insert ~~victims~~patients
        numberOfInserts = 0;
        try (Statement stmt = conn.createStatement()) {
            String sql = "INSERT INTO patient (id, name, salary) values(1, 'Dr. Dre', 10000000.02)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO patient (id, name, salary) values(2, 'Sir Albert Humbug McMiller', 53768.32)";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO patient (id, name, salary) values(3, 'Dr. Stephen Strange', 10000000.02)"; // Hold up a second
            numberOfInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(numberOfInserts, is(3));

        // Insert treatments
        numberOfInserts = 0;
        try (Statement stmt = conn.createStatement()) {
            String sql = "INSERT INTO treatment (id, name, doctor_fk, patient_fk, outcome) values(1, 'Bioelectromagnetic therapy', 2,1, 'Success, but 6 feet under')";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO treatment (id, name, doctor_fk, patient_fk, outcome) values(2, 'General Checkup', 3, 2, 'Mostly alive')";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO treatment (id, name, doctor_fk, patient_fk, outcome) values(3, 'Treatment with Daytrana', 3, 2, 'Fine')";
            numberOfInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO treatment (id, name, doctor_fk, patient_fk, outcome) values(4, 'Surgery', 1, 3, 'Alive')";
            numberOfInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        assertThat(numberOfInserts, is(4));
    }

    @Test
    public void Test10_verifyDoctorData() {
        // Verify doctors
        try (
                Statement stmt = conn.createStatement();
                ResultSet doctorResultSet = stmt.executeQuery("SELECT id, name, occupation, salary FROM DOCTOR ORDER BY ID")
        ) {


            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(1));
            assertThat(doctorResultSet.getString("name"), is("Dr. Stephen Strange"));
            assertThat(doctorResultSet.getString("occupation"), is("Surgeon"));
            assertThat(doctorResultSet.getDouble("salary"), is(10000000.02));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(2));
            assertThat(doctorResultSet.getString("name"), is("Dr. Wilhelm Mayor"));
            assertThat(doctorResultSet.getString("occupation"), is("Cardiologist"));
            assertThat(doctorResultSet.getDouble("salary"), is(300000.32));

            doctorResultSet.next();
            assertThat(doctorResultSet.getInt("id"), is(3));
            assertThat(doctorResultSet.getString("name"), is("Dr. Casey Washington"));
            assertThat(doctorResultSet.getString("occupation"), is("Veterinary physician"));
            assertThat(doctorResultSet.getDouble("salary"), is(60000.62));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test11_verifyPatientData() {
        // Verify patients
        try (
                Statement stmt = conn.createStatement();
                ResultSet patientResultSet = stmt.executeQuery("SELECT id, name, salary FROM PATIENT ORDER BY ID")) {

            patientResultSet.next();
            assertThat(patientResultSet.getInt("id"), is(1));
            assertThat(patientResultSet.getString("name"), is("Dr. Dre"));
            assertThat(patientResultSet.getDouble("salary"), is(10000000.02));

            patientResultSet.next();
            assertThat(patientResultSet.getInt("id"), is(2));
            assertThat(patientResultSet.getString("name"), is("Sir Albert Humbug McMiller"));
            assertThat(patientResultSet.getDouble("salary"), is(53768.32));

            patientResultSet.next();
            assertThat(patientResultSet.getInt("id"), is(3));
            assertThat(patientResultSet.getString("name"), is("Dr. Stephen Strange"));
            assertThat(patientResultSet.getDouble("salary"), is(10000000.02));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test12_verifyTreatmentData() {
        // Verify treatments
        try (
                Statement stmt = conn.createStatement();
                ResultSet treatmentResultSet = stmt.executeQuery("SELECT id, name, doctor_fk, patient_fk, outcome FROM TREATMENT ORDER BY ID")) {

            treatmentResultSet.next();
            assertThat(treatmentResultSet.getInt("id"), is(1));
            assertThat(treatmentResultSet.getString("name"), is("Bioelectromagnetic therapy"));
            assertThat(treatmentResultSet.getInt("doctor_fk"), is(2));
            assertThat(treatmentResultSet.getInt("patient_fk"), is(1));
            assertThat(treatmentResultSet.getString("outcome"), is("Success, but 6 feet under"));

            treatmentResultSet.next();
            assertThat(treatmentResultSet.getInt("id"), is(2));
            assertThat(treatmentResultSet.getString("name"), is("General Checkup"));
            assertThat(treatmentResultSet.getInt("doctor_fk"), is(3));
            assertThat(treatmentResultSet.getInt("patient_fk"), is(2));
            assertThat(treatmentResultSet.getString("outcome"), is("Mostly alive"));

            treatmentResultSet.next();
            assertThat(treatmentResultSet.getInt("id"), is(3));
            assertThat(treatmentResultSet.getString("name"), is("Treatment with Daytrana"));
            assertThat(treatmentResultSet.getInt("doctor_fk"), is(3));
            assertThat(treatmentResultSet.getInt("patient_fk"), is(2));
            assertThat(treatmentResultSet.getString("outcome"), is("Fine"));

            treatmentResultSet.next();
            assertThat(treatmentResultSet.getInt("id"), is(4));
            assertThat(treatmentResultSet.getString("name"), is("Surgery"));
            assertThat(treatmentResultSet.getInt("doctor_fk"), is(1));
            assertThat(treatmentResultSet.getInt("patient_fk"), is(3));
            assertThat(treatmentResultSet.getString("outcome"), is("Alive"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test20_verifyDoctorMetaData() {
        //Make sure to use ALL_CAPS!!
        HashMap<String, Integer> columnsDefinitions = new HashMap<>();
        columnsDefinitions.put("ID", Types.INTEGER);
        columnsDefinitions.put("NAME", Types.VARCHAR);
        columnsDefinitions.put("OCCUPATION", Types.VARCHAR);
        columnsDefinitions.put("SALARY", Types.DECIMAL);

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "DOCTOR", null);

            while(columns.next()) {
                String columnName = columns.getString(4);
                int columnType = columns.getInt(5);

                assertThat(columnsDefinitions.containsKey(columnName), is(true));
                assertThat(columnsDefinitions.get(columnName), is(columnType));

                columnsDefinitions.remove(columnName);
            }
            assertThat(columnsDefinitions.isEmpty(), is(true));
            assertThat(columns.next(), is(false));

            columns.close();

            ResultSet primaryKeyColumn = metaData.getPrimaryKeys(null, null, "DOCTOR");
            primaryKeyColumn.next();
            String primaryKeyName = primaryKeyColumn.getString(4);
            assertThat(primaryKeyName, is("ID"));

            primaryKeyColumn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test21_verifyPatientMetaData() {
        //Make sure to use ALL_CAPS!!
        HashMap<String, Integer> columnsDefinitions = new HashMap<>();
        columnsDefinitions.put("ID", Types.INTEGER);
        columnsDefinitions.put("NAME", Types.VARCHAR);
        columnsDefinitions.put("SALARY", Types.DECIMAL);

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "PATIENT", null);

            while(columns.next()) {
                String columnName = columns.getString(4);
                int columnType = columns.getInt(5);

                assertThat(columnsDefinitions.containsKey(columnName), is(true));
                assertThat(columnsDefinitions.get(columnName), is(columnType));

                columnsDefinitions.remove(columnName);
            }
            assertThat(columnsDefinitions.isEmpty(), is(true));
            assertThat(columns.next(), is(false));

            columns.close();

            ResultSet primaryKeyColumn = metaData.getPrimaryKeys(null, null, "PATIENT");
            primaryKeyColumn.next();
            String primaryKeyName = primaryKeyColumn.getString(4);
            assertThat(primaryKeyName, is("ID"));

            primaryKeyColumn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test22_verifyTreatmentMetaData() {
        //Make sure to use ALL_CAPS!!
        HashMap<String, Integer> columnsDefinitions = new HashMap<>();
        columnsDefinitions.put("ID", Types.INTEGER);
        columnsDefinitions.put("NAME", Types.VARCHAR);
        columnsDefinitions.put("DOCTOR_FK", Types.INTEGER);
        columnsDefinitions.put("PATIENT_FK", Types.INTEGER);
        columnsDefinitions.put("OUTCOME", Types.VARCHAR);

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, "TREATMENT", null);

            while(columns.next()) {
                String columnName = columns.getString(4);
                int columnType = columns.getInt(5);

                assertThat(columnsDefinitions.containsKey(columnName), is(true));
                assertThat(columnsDefinitions.get(columnName), is(columnType));

                columnsDefinitions.remove(columnName);
            }
            assertThat(columnsDefinitions.isEmpty(), is(true));
            assertThat(columns.next(), is(false));

            columns.close();

            ResultSet primaryKeyColumn = metaData.getPrimaryKeys(null, null, "TREATMENT");
            primaryKeyColumn.next();
            String primaryKeyName = primaryKeyColumn.getString(4);
            assertThat(primaryKeyName, is("ID"));

            primaryKeyColumn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
