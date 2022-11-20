package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.InstructorService;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
@ParametersAreNonnullByDefault
public class ImplementedInstructorService implements InstructorService {
    static String sta_insert,sta_queryAll;
    static {
        sta_insert = "insert into alluser values(?,1,?,null,null,?,?) ";
        sta_queryAll = "select b.id,b.name,b.totalcapacity,b.leftcapacity from "+
                "(select distinct coursesectionid from coursesectionclass where instructorid=?)a inner join " +
                "(select coursesection.id,coursesection.name,coursesection.totalcapacity,coursesection.leftcapacity from coursesection where coursesection.semesterid=?)b on a.coursesectionid=b.id";
    }


    @Override
    public  void addInstructor(int userId, String firstName, String lastName) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_insert=con.prepareStatement (ImplementedInstructorService.sta_insert);
            sta_insert.setInt(1,userId);
            boolean alpha=true;
            for(int i=0;i<firstName.length();i++){
                if (!((Character.isSpaceChar(firstName.charAt(i)))||(firstName.charAt(i) >= 'a' && firstName.charAt(i) <= 'z') || (firstName.charAt(i) >= 'A' && firstName.charAt(i) <= 'Z'))) {
                    alpha = false;
                    break;
                }
            }
            for(int i=0;i<lastName.length();i++){
                if (!((Character.isSpaceChar(lastName.charAt(i)))||(lastName.charAt(i) >= 'a' && lastName.charAt(i) <= 'z') || (lastName.charAt(i) >= 'A' && lastName.charAt(i) <= 'Z'))) {
                    alpha = false;
                    break;
                }
            }
            if(alpha){
                sta_insert.setString(2,firstName+" "+lastName);
            }else {
                sta_insert.setString(2,firstName+lastName);
            }
            sta_insert.setString(3,firstName);
            sta_insert.setString(4,lastName);
            sta_insert.execute();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
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
    public  List< CourseSection > getInstructedCourseSections(int instructorId, int semesterId) {
        Connection con=null;
        List<CourseSection> list = new ArrayList<>();
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryAll=con.prepareStatement (ImplementedInstructorService.sta_queryAll);
            sta_queryAll.setInt(1, instructorId);
            sta_queryAll.setInt(2, semesterId);
            CourseSection here;
            ResultSet resultSet = sta_queryAll.executeQuery();
            while (resultSet.next()){
                here = new CourseSection();
                here.id = resultSet.getInt(1);
                here.name = resultSet.getString(2);
                here.totalCapacity = resultSet.getInt(3);
                here.leftCapacity = resultSet.getInt(4);
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
}
