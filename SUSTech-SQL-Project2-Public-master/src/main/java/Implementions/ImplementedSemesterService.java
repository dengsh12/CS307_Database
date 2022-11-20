package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Semester;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.SemesterService;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
@ParametersAreNonnullByDefault
public class ImplementedSemesterService implements SemesterService {
    static String sta_insert;
    static String sta_delete;
    static String sta_queryAll;
    static String sta_queryId;
    static {
        sta_insert = "insert into semester values (default,?,?,?)";
        sta_delete = "delete from semester where id=?";
        sta_queryAll = "select * from semester";
        sta_queryId = "select * from semester where id=?";
    }
    @Override
    public int addSemester(String name, Date begin, Date end) {
        Connection con=null;
        try {
            if(begin.after(end)){
                throw new IntegrityViolationException();
            }
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_insert=con.prepareStatement (ImplementedSemesterService.sta_insert,PreparedStatement.RETURN_GENERATED_KEYS);
            sta_insert.setString(1, name);
            sta_insert.setDate(2, begin);
            sta_insert.setDate(3, end);
            sta_insert.executeUpdate();
            ResultSet resultSet=sta_insert.getGeneratedKeys();
            if(resultSet.next()){
                int a=resultSet.getInt(1);
                con.commit();
                con.close();
                return a;
            }else {
                con.commit();
                con.close();
                throw new IntegrityViolationException();
            }
        } catch (SQLException e) {
            try {
                con.commit();
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw new IntegrityViolationException();
        }
    }
    @Override
    public void removeSemester(int semesterId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_delete=con.prepareStatement (ImplementedSemesterService.sta_delete);
            sta_delete.setInt(1, semesterId);
            int cnt=sta_delete.executeUpdate();
            if(cnt==0){
                con.commit();
                con.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            try {
                con.commit();
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    @Override
    public  List< Semester > getAllSemesters() {
        Connection con=null;
        List<Semester> list = new ArrayList<>();
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryAll=con.prepareStatement (ImplementedSemesterService.sta_queryAll);
            ResultSet result = sta_queryAll.executeQuery();
            Semester here;
            while (result.next()){
                here = new Semester();
                here.id = result.getInt(1);
                here.name = result.getString(2);
                here.begin = result.getDate(3);
                here.end = result.getDate(4);
                list.add(here);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return list;
    }

    @Override
    public  Semester getSemester(int semesterId) {
        Semester here = null;
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryId=con.prepareStatement (ImplementedSemesterService.sta_queryId);
            sta_queryId.setInt(1, semesterId);
            ResultSet result = sta_queryId.executeQuery();
            if(result.next()){
                here = new Semester();
                here.id = result.getInt(1);
                here.name = result.getString(2);
                here.begin = result.getDate(3);
                here.end = result.getDate(4);
                con.commit();
                con.close();
                return here;
            }else {
                try {
                    con.commit();
                    con.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        if(here==null){
            throw new EntityNotFoundException();
        }
        return here;
    }
}
