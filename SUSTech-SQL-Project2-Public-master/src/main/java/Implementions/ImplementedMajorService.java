package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.MajorService;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
@ParametersAreNonnullByDefault
public class ImplementedMajorService implements MajorService {
    static String sta_insert,sta_remove,sta_queryAll,sta_queryById,
            sta_majorCompulsoryCourse, sta_majorElectiveCourse;
    static {
            sta_insert = "insert into major values (default,?,?)";
            sta_remove = "delete from major where id=?";
            sta_queryAll = "select m.id, m.name, m.departmentid, d.name from major m inner join department d on m.departmentid = d.id";
            sta_queryById = "select m.id, m.name, m.departmentid, d.name from (select * from major where id=?)m inner join department d on m.departmentid=d.id";
            sta_majorCompulsoryCourse = "insert into coursetomajor values (?,?,1)";
            sta_majorElectiveCourse = "insert into coursetomajor values (?,?,0)";
    }
    @Override
    public int addMajor(String name, int departmentId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con.setAutoCommit(false);
            PreparedStatement sta_insert=con.prepareStatement
                    (ImplementedMajorService.sta_insert,PreparedStatement.RETURN_GENERATED_KEYS);
            sta_insert.setString(1, name);
            sta_insert.setInt(2, departmentId);
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
            e.printStackTrace();
            try {
                con.commit();
                con.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void removeMajor(int majorId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con.setAutoCommit(false);
            PreparedStatement sta_remove=con.prepareStatement (ImplementedMajorService.sta_remove);
            sta_remove.setInt(1, majorId);
            int cnt=sta_remove.executeUpdate();
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
    public  List< Major > getAllMajors() {
        Connection con=null;
        List<Major> list = new ArrayList<>();
        try {
            PreparedStatement sta_queryAll=con.prepareStatement (ImplementedMajorService.sta_queryAll);
            ResultSet resultSet = sta_queryAll.executeQuery();
            Major here;
            while (resultSet.next()){
                here = new Major();
                here.id = resultSet.getInt(1);
                here.name = resultSet.getString(2);
                here.department = new Department();
                here.department.id = resultSet.getInt(3);
                here.department.name = resultSet.getString(4);
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
    public  Major getMajor(int majorId) {
        Connection con=null;
        Major major=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con.setAutoCommit(false);
            PreparedStatement sta_queryById=con.prepareStatement (ImplementedMajorService.sta_queryById);
            sta_queryById.setInt(1, majorId);
            ResultSet resultSet = sta_queryById.executeQuery();
            if(resultSet.next()){
                major = new Major();
                major.id = resultSet.getInt(1);
                major.name = resultSet.getString(2);
                major.department = new Department();
                major.department.id = resultSet.getInt(3);
                major.department.name = resultSet.getString(4);
                con.commit();
                con.close();
                return major;
            }else {
                con.commit();
                con.close();
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
        if(major == null){
            throw new EntityNotFoundException();
        }
        return major;
    }

    @Override
    public  void addMajorCompulsoryCourse(int majorId, String courseId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con.setAutoCommit(false);
            PreparedStatement sta_majorCompulsoryCourse=con.prepareStatement (ImplementedMajorService.sta_majorCompulsoryCourse);
            sta_majorCompulsoryCourse.setString(1, courseId);
            sta_majorCompulsoryCourse.setInt(2, majorId);
            sta_majorCompulsoryCourse.execute();
            con.commit();
            con.close();
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
    public  void addMajorElectiveCourse(int majorId, String courseId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con.setAutoCommit(false);
            PreparedStatement sta_majorElectiveCourse=con.prepareStatement (ImplementedMajorService.sta_majorElectiveCourse);
            sta_majorElectiveCourse.setString(1, courseId);
            sta_majorElectiveCourse.setInt(2, majorId);
            sta_majorElectiveCourse.execute();
            con.commit();
            con.close();
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
}
