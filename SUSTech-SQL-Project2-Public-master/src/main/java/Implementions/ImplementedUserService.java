package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ImplementedUserService implements UserService {
    static String sta_delete;
    static String sta_queryAll;
    static String sta_queryId;
    static {
        sta_queryAll = "select alluser.id, alluser.role, alluser.fullname, alluser.enrolleddate, alluser.majorid, m.name, d.id, d.name"+
                " from alluser left join major m on alluser.majorid = m.id left join department d on m.departmentid = d.id";
        sta_delete = "delete from alluser where id=?";
        sta_queryId = "select a.id, role, fullname, enrolleddate, majorid,m.name,d.id,d.name"+
                " from (select * from alluser where id=?)a " +
                "left join major m on a.majorid=m.id " +
                "left join department d on m.departmentid = d.id";
    }
    @Override
    public  void removeUser(int userId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_delete=con.prepareStatement (ImplementedUserService.sta_delete);
            sta_delete.setInt(1, userId);
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
    public  List< User > getAllUsers() {
        Connection con=null;
        List<User> list = new ArrayList<>();
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryAll=con.prepareStatement (ImplementedUserService.sta_queryAll);
            ResultSet resultSet = sta_queryAll.executeQuery();
            Student student;
            Instructor instructor;
            while (resultSet.next()){
                if(resultSet.getInt(2)==0){
                    student = new Student();
                    student.id = resultSet.getInt(1);
                    student.fullName = resultSet.getString(3);
                    student.enrolledDate = resultSet.getDate(4);
                    student.major = new Major();
                    student.major.id = resultSet.getInt(5);
                    student.major.name = resultSet.getString(6);
                    student.major.department = new Department();
                    student.major.department.id = resultSet.getInt(7);
                    student.major.department.name = resultSet.getString(8);
                    list.add(student);
                }else {
                    instructor = new Instructor();
                    instructor.id = resultSet.getInt(1);
                    instructor.fullName = resultSet.getString(3);
                    list.add(instructor);
                }
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
    public  User getUser(int userId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryId=con.prepareStatement (ImplementedUserService.sta_queryId);
            sta_queryId.setInt(1, userId);
            ResultSet resultSet = sta_queryId.executeQuery();
            if(resultSet.next()){
                if(resultSet.getInt(2)==0){
                    Student student = new Student();
                    student.id = resultSet.getInt(1);
                    student.fullName = resultSet.getString(3);
                    student.enrolledDate = resultSet.getDate(4);
                    student.major = new Major();
                    student.major.id = resultSet.getInt(5);
                    student.major.name = resultSet.getString(6);
                    student.major.department = new Department();
                    student.major.department.id = resultSet.getInt(7);
                    student.major.department.name = resultSet.getString(8);
                    con.commit();
                    con.close();
                    return student;
                }else {
                    Instructor instructor;
                    instructor = new Instructor();
                    instructor.id = resultSet.getInt(1);
                    instructor.fullName = resultSet.getString(3);
                    con.commit();
                    con.close();
                    return instructor;
                }
            }else {
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
        throw new EntityNotFoundException();
    }
}
