package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.CourseService;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.time.DayOfWeek;
import java.util.*;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public class ImplementedCourseService implements CourseService {
    public static int num = 0;
    static String sta_insertCourse, sta_insertCourseSection, sta_insertCourseSectionClass,
            sta_removeCourse, sta_removeCourseSection, sta_removeCourseSectionClass, sta_courseSectionInSemester,
            sta_getCourseBySection, sta_sectionClassBySectionId, sta_getSectionByClass, sta_enrollStudentInSemester, sta_queryAllCourse;
    static PrerequisiteAndString cases = new PrerequisiteAndString ();

    static {


        sta_insertCourse = "insert into course values (?,?,?,?,?,?)";
        sta_insertCourseSection = "insert into coursesection values (default,?,?,?,?,?)";
//            Statement.RETURN_GENERATED_KEYS;
        sta_insertCourseSectionClass = "insert into coursesectionclass values (default,?,?,?,?,?,?,?)";
//            Statement.RETURN_GENERATED_KEYS;
        sta_removeCourse = "delete from course where id=?";
        sta_removeCourseSection = "delete from coursesection where id=?";
        sta_removeCourseSectionClass = "delete from coursesectionclass where id=?";
        sta_courseSectionInSemester = "select id,name,totalcapacity,leftcapacity from coursesection where courseid=? and semesterid=?";
        sta_getCourseBySection = "select * from course where id = (select courseid from coursesection where id=?)";
        sta_sectionClassBySectionId = "select a.id, dayofweek, weeklist, classbegin, classend, location,b.id,b.fullname " +
                "from (select * from coursesectionclass where coursesectionid=?) a join alluser b on instructorid=b.id";
        sta_getSectionByClass = "select id, name, totalcapacity, leftcapacity from coursesection where id=(select coursesectionid from coursesectionclass where coursesectionclass.id=?)";
        sta_enrollStudentInSemester = "select userId, d.fullname, d.enrolleddate, d.majorid, d.name, d.departmentid, department.name  from " +
                "(select userId, fullname, enrolleddate, majorid,name,departmentid from (select id as userId, fullname, enrolleddate, majorid from (select studentid from (select id from coursesection where courseid=? and semesterid=?)a " +
                "join studenttocoursesection on a.id=studenttocoursesection.coursesectionid) b join alluser on b.studentid=alluser.id)c join major on c.majorId=major.id)d " +
                "join department on d.departmentid=department.id";
        sta_queryAllCourse = "select id,name,credit,classhour,coursegrading from course";

    }

    @Override
    public void addCourse(String courseId, String courseName, int credit, int classHour, Course.CourseGrading grading, @Nullable Prerequisite coursePrerequisite) {
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_insertCourse = con.prepareStatement (ImplementedCourseService.sta_insertCourse);
            sta_insertCourse.setString (1, courseId);
            sta_insertCourse.setString (2, courseName);
            sta_insertCourse.setInt (3, credit);
            sta_insertCourse.setInt (4, classHour);
            sta_insertCourse.setInt (5, (grading == Course.CourseGrading.PASS_OR_FAIL) ? 0 : 1);
            if (coursePrerequisite == null) {
                sta_insertCourse.setString (6, null);
            } else {
                sta_insertCourse.setString (6, coursePrerequisite.when (cases));
            }
            sta_insertCourse.execute ();
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
            try {
                con.rollback ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            throw new IntegrityViolationException ();
        }
    }

    @Override
    public int addCourseSection(String courseId, int semesterId, String sectionName, int totalCapacity) {
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_insertCourseSection = con.prepareStatement (
                    ImplementedCourseService.sta_insertCourseSection, PreparedStatement.RETURN_GENERATED_KEYS);
            sta_insertCourseSection.setString (1, sectionName);
            sta_insertCourseSection.setString (2, courseId);
            sta_insertCourseSection.setInt (3, totalCapacity);
            sta_insertCourseSection.setInt (4, totalCapacity);
            sta_insertCourseSection.setInt (5, semesterId);
            sta_insertCourseSection.executeUpdate ();
            ResultSet resultSet = sta_insertCourseSection.getGeneratedKeys ();
            if (resultSet.next ()) {
                int a = resultSet.getInt (1);
                con.commit ();
                con.close ();
                return a;
            } else {
                con.commit ();
                con.close ();
                throw new IntegrityViolationException ();
            }
        } catch (SQLException e) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            e.printStackTrace ();
            throw new IntegrityViolationException ();
        }

    }

    @Override
    public int addCourseSectionClass(int sectionId, int instructorId, DayOfWeek dayOfWeek, Set< Short > weekList, short classStart, short classEnd, String location) {
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            if (classEnd < classStart) {
                con.commit();
                con.close();
                throw new IntegrityViolationException ();
            }
            PreparedStatement sta_insertCourseSectionClass = con.prepareStatement (
                    ImplementedCourseService.sta_insertCourseSectionClass, Statement.RETURN_GENERATED_KEYS);
            sta_insertCourseSectionClass.setInt (1, instructorId);
            switch (dayOfWeek) {
                case MONDAY:
                    sta_insertCourseSectionClass.setInt (2, 1);
                    break;
                case TUESDAY:
                    sta_insertCourseSectionClass.setInt (2, 2);
                    break;
                case WEDNESDAY:
                    sta_insertCourseSectionClass.setInt (2, 3);
                    break;
                case THURSDAY:
                    sta_insertCourseSectionClass.setInt (2, 4);
                    break;
                case FRIDAY:
                    sta_insertCourseSectionClass.setInt (2, 5);
                    break;
                case SATURDAY:
                    sta_insertCourseSectionClass.setInt (2, 6);
                    break;
                case SUNDAY:
                    sta_insertCourseSectionClass.setInt (2, 7);
                    break;
            }
            sta_insertCourseSectionClass.setArray (3, con.createArrayOf ("smallint", weekList.toArray ()));
            sta_insertCourseSectionClass.setShort (4, classStart);
            sta_insertCourseSectionClass.setShort (5, classEnd);
            sta_insertCourseSectionClass.setString (6, location);
            sta_insertCourseSectionClass.setInt (7, sectionId);
            sta_insertCourseSectionClass.executeUpdate ();
            ResultSet resultSet = sta_insertCourseSectionClass.getGeneratedKeys ();
            if (resultSet.next ()) {
                int a = resultSet.getInt (1);
                con.commit ();
                con.close ();
                return a;
            } else {
                con.commit ();
                con.close ();
                throw new IntegrityViolationException ();
            }
        } catch (SQLException e) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            e.printStackTrace ();
            throw new IntegrityViolationException ();
        }

    }

    @Override
    public void removeCourse(String courseId) {
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_removeCourse = con.prepareStatement (ImplementedCourseService.sta_removeCourse);
            sta_removeCourse.setString (1, courseId);
            int cnt = sta_removeCourse.executeUpdate ();
            if (cnt == 0) {
                con.commit ();
                con.close ();
                throw new EntityNotFoundException ();
            }
            con.commit ();
            con.close ();

        } catch (SQLException e) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            e.printStackTrace ();
        }
    }

    @Override
    public void removeCourseSection(int sectionId) {
        Connection con = null;

        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_removeCourseSection = con.prepareStatement (ImplementedCourseService.sta_removeCourseSection);
            sta_removeCourseSection.setInt (1, sectionId);
            int cnt = sta_removeCourseSection.executeUpdate ();
            if (cnt == 0) {
                con.commit ();
                con.close ();
                throw new EntityNotFoundException ();
            }
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            e.printStackTrace ();
        }
    }

    @Override
    public void removeCourseSectionClass(int classId) {
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_removeCourseSectionClass =
                    con.prepareStatement (ImplementedCourseService.sta_removeCourseSectionClass);
            sta_removeCourseSectionClass.setInt (1, classId);
            int cnt = sta_removeCourseSectionClass.executeUpdate ();
            if (cnt == 0) {
                con.commit ();
                con.close ();
                throw new EntityNotFoundException ();
            }
        } catch (SQLException e) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException ex) {
                ex.printStackTrace ();
            }
            e.printStackTrace ();
        }
    }

    @Override
    public List< Course > getAllCourses() {
        Connection con = null;
        List< Course > list = new ArrayList<> ();
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_queryAllCourse = con.prepareStatement (ImplementedCourseService.sta_queryAllCourse);
            ResultSet resultSet = sta_queryAllCourse.executeQuery ();
            while (resultSet.next ()) {
//                id,name,credit,classhour,coursegrading
                Course here = new Course ();
                here.id = resultSet.getString (1);
                here.name = resultSet.getString (2);
                here.credit = resultSet.getInt (3);
                here.classHour = resultSet.getInt (4);
                here.grading = resultSet.getInt (5) == 1 ? Course.CourseGrading.HUNDRED_MARK_SCORE : Course.CourseGrading.PASS_OR_FAIL;
                list.add (here);
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        return list;
    }

    @Override
    public List< CourseSection > getCourseSectionsInSemester(String courseId, int semesterId) {
        List< CourseSection > list = new ArrayList<> ();
        Connection con = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_courseSectionInSemester = con.prepareStatement (ImplementedCourseService.sta_courseSectionInSemester);
            sta_courseSectionInSemester.setString (1, courseId);
            sta_courseSectionInSemester.setInt (2, semesterId);
            CourseSection here;
            ResultSet resultSet = sta_courseSectionInSemester.executeQuery ();
            while (resultSet.next ()) {
                here = new CourseSection ();
                here.id = resultSet.getInt (1);
                here.name = resultSet.getString (2);
                here.totalCapacity = resultSet.getInt (3);
                here.leftCapacity = resultSet.getInt (4);
                list.add (here);
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        return list;
    }

    @Override
    public Course getCourseBySection(int sectionId) {
        Connection con = null;
        Course here = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_getCourseBySection = con.prepareStatement (ImplementedCourseService.sta_getCourseBySection);
            sta_getCourseBySection.setInt (1, sectionId);
            ResultSet resultSet = sta_getCourseBySection.executeQuery ();
            if (resultSet.next ()) {
                here = new Course ();
                here.id = resultSet.getString (1);
                here.name = resultSet.getString (2);
                here.credit = resultSet.getInt (3);
                here.classHour = resultSet.getInt (4);
                here.grading = resultSet.getInt (5) == 0 ? Course.CourseGrading.PASS_OR_FAIL : Course.CourseGrading.HUNDRED_MARK_SCORE;
            } else {
                con.commit ();
                con.close ();
                throw new EntityNotFoundException ();
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        if (here == null) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException e) {
                e.printStackTrace ();
            }
            throw new EntityNotFoundException ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        return here;
    }

    @Override
    public List< CourseSectionClass > getCourseSectionClasses(int sectionId) {
        Connection con = null;
        List< CourseSectionClass > list = new ArrayList<> ();
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
//            select a.id, dayofweek, weeklist, classbegin, classend, location,b.id,b.fullname
            PreparedStatement sta_sectionClassBySectionId = con.prepareStatement (ImplementedCourseService.sta_sectionClassBySectionId);
            sta_sectionClassBySectionId.setInt (1, sectionId);
            ResultSet resultSet = sta_sectionClassBySectionId.executeQuery ();
            CourseSectionClass here;
            Instructor instructor;
            while (resultSet.next ()) {
                here = new CourseSectionClass ();
                here.id = resultSet.getInt (1);
                here.dayOfWeek = DayOfWeek.SUNDAY.plus (resultSet.getInt (2));
                here.weekList = Arrays.stream ((Short[]) resultSet.getArray (3).getArray ()).collect (Collectors.toCollection (HashSet::new));
                here.classBegin = resultSet.getShort (4);
                here.classEnd = resultSet.getShort (5);
                here.location = resultSet.getString (6);
                instructor = new Instructor ();
                instructor.id = resultSet.getInt (7);
                instructor.fullName = resultSet.getString (8);
                here.instructor = instructor;
                list.add (here);
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }

        return list;
    }

    @Override
    public CourseSection getCourseSectionByClass(int classId) {
//        sta_getSectionByClass="select id, name, totalcapacity, leftcapacity from coursesection where id=(select coursesectionid from coursesectionclass where coursesectionclass.id=?)";
        Connection con = null;
        CourseSection here = null;
        try {
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_getSectionByClass = con.prepareStatement (ImplementedCourseService.sta_getSectionByClass);
            sta_getSectionByClass.setInt (1, classId);
            ResultSet resultSet = sta_getSectionByClass.executeQuery ();
            if (resultSet.next ()) {
                here = new CourseSection ();
                here.id = resultSet.getInt (1);
                here.name = resultSet.getString (2);
                here.totalCapacity = resultSet.getInt (3);
                here.leftCapacity = resultSet.getInt (4);
            } else {
                try {
                    con.commit ();
                    con.close ();
                } catch (SQLException e) {
                    e.printStackTrace ();
                }
                throw new EntityNotFoundException ();
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        if (here == null) {
            try {
                con.commit ();
                con.close ();
            } catch (SQLException e) {
                e.printStackTrace ();
            }
            throw new EntityNotFoundException ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        return here;
    }

    @Override
    public List< Student > getEnrolledStudentsInSemester(String courseId, int semesterId) {
        List< Student > list = new ArrayList<> ();
        Connection con = null;
        try {
//            sta_enrollStudentInSemester="select userId, d.fullname, d.enrolleddate, d.majorid, d.name, d.departmentid, department.name  from "+
//                    "(select userId, fullname, enrolleddate, majorid,name,departmentid from (select id as userId, fullname, enrolleddate, majorid from (select studentid from (select id from coursesection where courseid=? and semesterid=?)a " +
//                    "join studenttocoursesection on a.id=studenttocoursesection.coursesectionid) b join alluser on b.studentid=alluser.id)c join major on c.majorId=major.id)d " +
//                    "join department on d.departmentid=department.id");
            con = SQLDataSource.getInstance ().getSQLConnection ();
            con.setTransactionIsolation (Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit (false);
            PreparedStatement sta_enrollStudentInSemester = con.prepareStatement (ImplementedCourseService.sta_enrollStudentInSemester);
            sta_enrollStudentInSemester.setString (1, courseId);
            sta_enrollStudentInSemester.setInt (2, semesterId);
            ResultSet resultSet = sta_enrollStudentInSemester.executeQuery ();
            Student here = null;
            while (resultSet.next ()) {
                here = new Student ();
                here.id = resultSet.getInt (1);
                here.fullName = resultSet.getString (2);
                here.enrolledDate = resultSet.getDate (3);
                here.major = new Major ();
                here.major.id = resultSet.getInt (4);
                here.major.name = resultSet.getString (5);
                here.major.department = new Department ();
                here.major.department.id = resultSet.getInt (6);
                here.major.department.name = resultSet.getString (7);
                list.add (here);
            }
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        try {
            con.commit ();
            con.close ();
        } catch (SQLException e) {
            e.printStackTrace ();
        }
        return list;
    }
}
