package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
@ParametersAreNonnullByDefault
public class ImplementedDepartmentService implements DepartmentService {
    public static String sta_insert;
    static String sta_delete;
    static String sta_queryAll;
    static String sta_queryId;
    static {

        sta_insert = "insert into department values (default,?)";
        sta_delete = "delete from department where id=?";
        sta_queryAll = "select * from department";
        sta_queryId = "select * from department where id=?";

    }
    @Override
    public int addDepartment(String name) {
        Connection con=null;
        ResultSet result = null;
        int row = 0;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_insert=con.prepareStatement (ImplementedDepartmentService.sta_insert,PreparedStatement.RETURN_GENERATED_KEYS);
            sta_insert.setString(1, name);
            row = sta_insert.executeUpdate();
            if(row==0){
                con.commit();
                con.close();
                throw new IntegrityViolationException();
            }
            result = sta_insert.getGeneratedKeys();
            result.next();
            int a=result.getInt(1);
            con.commit();
            con.close();
            return a;
        } catch (SQLException e) {
            try {
                con.commit();
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            throw new IntegrityViolationException();
        }
    }

    @Override
    public  void removeDepartment(int departmentId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_delete=con.prepareStatement (ImplementedDepartmentService.sta_delete);
            sta_delete.setInt(1, departmentId);
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
    public  List< Department > getAllDepartments() {
        Connection con=null;
        List<Department> list = new ArrayList<>();
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryAll=con.prepareStatement (ImplementedDepartmentService.sta_queryAll);
            ResultSet result = sta_queryAll.executeQuery();
            Department here;
            while (result.next()){
                here = new Department();
                here.id = result.getInt(1);
                here.name = result.getString(2);
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
    public  Department getDepartment(int departmentId) {
        Connection con=null;
        Department here = null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryId=con.prepareStatement (ImplementedDepartmentService.sta_queryId);
            sta_queryId.setInt(1, departmentId);
            ResultSet result = sta_queryId.executeQuery();
            if(result.next()){
                here = new Department();
                here.id = result.getInt(1);
                here.name = result.getString(2);
            }else {
                con.commit();
                con.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(here == null){
            try {
                con.commit();
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw new EntityNotFoundException();
        }
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return here;
    }
}
