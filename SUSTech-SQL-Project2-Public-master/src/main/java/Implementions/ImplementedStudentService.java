package Implementions;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.StudentService;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Date;
import java.sql.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

class JudgeConflictEntry{
    Short[] weekList;
    int dayOfWeek;
    short begin,end;
}

@ParametersAreNonnullByDefault
public class ImplementedStudentService implements StudentService {
    static boolean hasSame(Short[] arr1,Short[] arr2){
        int i=0,j=0;
        for(i=0;i<arr1.length;i++){
            while (arr1[i]>arr2[j]){
                j++;
                if(j==arr2.length)return false;
            }
            if(arr1[i].equals(arr2[j]))return true;
        }
        return false;
    }
    static boolean hasConflict(@Nullable JudgeConflictEntry entry1,@Nullable JudgeConflictEntry entry2){
        if((entry1==null)||(entry2==null)){
            return false;
        }
        if(hasSame(entry1.weekList,entry2.weekList)){
            if(entry1.dayOfWeek==entry2.dayOfWeek){
                return (entry1.end >= entry2.begin) && (entry1.begin <= entry2.end);
            }
        }
        return false;
    }
    static boolean containsInstructor(String firstName,String lastName,String instructorName){
        String[] names=new String[4];
        names[0]=firstName+lastName;
        names[1]=firstName+" "+lastName;
        names[2]=firstName;
        names[3]=lastName;
        for (int i=0;i<4;i++){
            if(names[i].indexOf(instructorName)==0){
                return true;
            }
        }
        return false;
    }
    static String sta_addStudent, sta_queryGrade,sta_dropCourse, sta_subCapacity,sta_enroll,
            sta_updGrade,sta_courseAndGrades,sta_courseAndGradesWithSemester,sta_queryCourseTable,sta_studentMajor,
            sta_queryPre,sta_queryHasPass,sta_queryCourseAndSection,sta_queryCourseSection,sta_queryMajorCompulsory,sta_queryMajorElective,sta_queryNotInMajor,
            sta_queryCourseInMajor, sta_queryStudentSelectedCourseInSemester, sta_addCapacity,sta_querySection, getSta_queryCourseSectionWithoutClass;
    static String[] sta_searchCourse=new String[8];
    static {
        sta_querySection="select leftcapacity,prerequisite,name from (select leftcapacity,courseid from coursesection where id=?)a join course on a.courseid=course.id";
        sta_addStudent="insert into alluser values (?,0,?,?,?,?,?)";
        sta_queryGrade ="select s.grade  from studenttocoursesection s where studentid=? and coursesectionid=?";
        sta_dropCourse="delete from studenttocoursesection where studentId=? and coursesectionid=?";
        sta_subCapacity ="update coursesection set leftcapacity=leftcapacity-1 where id=?";
        sta_enroll="insert into studenttocoursesection values (?,?,?)";
        sta_updGrade="update studenttocoursesection set grade=? where studentid=? and coursesectionid=?";

        sta_courseAndGrades="select c.grade,d.id,d.name,credit, classhour, coursegrading from "+
                "(select * from (select * from studenttocoursesection where studentid=?)a join coursesection b on a.coursesectionid=b.id join semester s on b.semesterid = s.id)c join course d on c.courseid=d.id order by c.begindate desc ";
        sta_courseAndGradesWithSemester="select c.grade,d.id,d.name,credit, classhour, coursegrading from"+
                "(select * from (select * from studenttocoursesection where studentid=?)a join ( select * from coursesection where semesterid=?) b on a.coursesectionid=b.id)c join course d on c.courseid=d.id ";

        sta_queryCourseTable="select c2.dayofweek,c2.weeklist,c.id,a2.id,a2.fullname,c2.classbegin,c2.classend,c2.location,s.begindate,s.enddate,c.name,b.name from(select * from studenttocoursesection where studentid=?)a"+
                " join courseSection b on a.coursesectionid=b.id join course c on b.courseid = c.id join coursesectionclass c2 on b.id = c2.coursesectionid join alluser a2 on c2.instructorid = a2.id join semester s on b.semesterid = s.id";

        sta_studentMajor="select m.id,m.name,d.id,d.name from (select majorid from alluser where id=?)a join major m on a.majorid=m.id join department d on m.departmentid=d.id";
        sta_queryPre="select prerequisite from course where id=?";

        sta_queryHasPass="select c.courseid from (select coursesectionid from studenttocoursesection where studentid=? and (grade>=60 or grade=-1))b join coursesection c"+
                " on coursesectionid=c.id";

        sta_queryCourseAndSection="select a.leftcapacity,c.prerequisite,a.semesterid,c.name,c2.weeklist,c2.classbegin,c2.classend,c2.dayofweek,c.id from (select leftcapacity,courseid,semesterid,id from coursesection where id=?)a "+
                "join course c on a.courseid=c.id left join coursesectionclass c2 on c2.coursesectionid=a.id";
        getSta_queryCourseSectionWithoutClass ="select a.grade,b.id, name, courseid, totalcapacity, leftcapacity, semesterid"+
                " from (select grade,coursesectionid from studenttocoursesection where studentid=?)a" +
                " join coursesection b on a.coursesectionid=b.id";
        sta_queryCourseSection="select a.grade,b.id, name, courseid, totalcapacity, leftcapacity, semesterid,c.weeklist,c.dayofweek,c.classbegin,c.classend"+
                " from (select grade,coursesectionid from studenttocoursesection where studentid=?)a" +
                " join coursesection b on a.coursesectionid=b.id left join coursesectionclass c on b.id = c.coursesectionid";

        sta_searchCourse[0]="select c.id,c.name,c.credit,c.classHour,c.courseGrading,c2.id,c2.name,"+
                "c2.totalCapacity,c2.leftCapacity,c3.id,c3.weekList,c3.classBegin,c3.classEnd,c3.location,a.id,a.fullName,c.prerequisite,c3.dayofweek,a.firstname,a.lastname " +
                "from course c join (select * from coursesection where semesterid=?)c2  on c.id = c2.courseid left join coursesectionclass c3 on c2.id = c3.coursesectionid " +
                "join alluser a on c3.instructorid = a.id order by (c.id,c.name||'['||c2.name||']',c2.id) ";
        sta_searchCourse[1]="select c.id,c.name,c.credit,c.classHour,c.courseGrading,c2.id,c2.name,c2.totalCapacity"+
                ",c2.leftCapacity,c3.id,c3.weekList,c3.classBegin,c3.classEnd,c3.location,a.id,a.fullName,c.prerequisite,c3.dayofweek,a.firstname,a.lastname  from" +
                " course c join (select * from coursesection where semesterid=?)c2 on c.id = c2.courseid left join coursesectionclass c3 on c2.id = c3.coursesectionid " +
                "join alluser a on c3.instructorid = a.id where position(? in c.name||'['||c2.name||']')>0 order by (c.id,c.name||'['||c2.name||']',c2.id)";
        sta_searchCourse[2]="select c.id,c.name,c.credit,c.classHour,c.courseGrading,c2.id,c2.name,"+
                "c2.totalCapacity,c2.leftCapacity,c3.id,c3.weekList,c3.classBegin,c3.classEnd,c3.location,a.id,a.fullName,c.prerequisite,c3.dayofweek,a.firstname,a.lastname  " +
                "from course c join (select * from coursesection where semesterid=?)c2 on c.id = c2.courseid left join coursesectionclass c3 " +
                "on c2.id = c3.coursesectionid join alluser a on c3.instructorid = a.id where position(? in c.id)>0 order by (c.id,c.name||'['||c2.name||']',c2.id)";
        sta_searchCourse[3]="select c.id,c.name,c.credit,c.classHour,c.courseGrading,c2.id,c2.name,"+
                "c2.totalCapacity,c2.leftCapacity,c3.id,c3.weekList,c3.classBegin,c3.classEnd,c3.location,a.id,a.fullName,c.prerequisite,c3.dayofweek,a.firstname,a.lastname  " +
                "from course c join (select * from coursesection where semesterid=?)c2 on c.id = c2.courseid left join coursesectionclass c3 " +
                "on c2.id = c3.coursesectionid join alluser a on c3.instructorid = a.id where position(? in c.id)>0 and position(? in c.name||'['||c2.name||']')>0 order by " +
                "(c.id,c.name||'['||c2.name||']',c2.id)";
        sta_queryMajorCompulsory="select courseid from coursetomajor where majorid=(select alluser.majorid from alluser where id=?) and relation=1";
        sta_queryMajorElective="select courseid from coursetomajor where majorid=(select alluser.majorid from alluser where id=?) and relation=0";
        sta_queryNotInMajor="select c.id from course c where c.id not in (select courseId from courseToMajor); ";
        sta_queryCourseInMajor="select courseid from coursetomajor where majorid=(select alluser.majorid from alluser where id=?)";
        sta_queryStudentSelectedCourseInSemester ="select class.weeklist,class.dayofweek,class.classbegin,class.classend,d.name,c.name,d.id from "+
                "(select * from studenttocoursesection where studentid=?)s join" +
                " (select * from coursesection where semesterid=?)c on s.coursesectionid=c.id left join coursesectionclass class on class.coursesectionid=c.id join course d on c.courseid=d.id";
        sta_addCapacity ="update coursesection set leftcapacity=leftcapacity+1 where id=?";
    }
    @Override
    public  void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_addStudent=con.prepareStatement (ImplementedStudentService.sta_addStudent);
            sta_addStudent.setInt(1, userId);
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
                sta_addStudent.setString(2,firstName+" "+lastName);
            }else {
                sta_addStudent.setString(2,firstName+lastName);
            }
            sta_addStudent.setDate(3, enrolledDate);
            sta_addStudent.setInt(4, majorId);
            sta_addStudent.setString(5, firstName);
            sta_addStudent.setString(6, lastName);
            sta_addStudent.execute();
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
    public  List< CourseSearchEntry > searchCourse(int studentId, int semesterId, @Nullable String searchCid, @Nullable String searchName,
                                                   @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime,
                                                   @Nullable List< String > searchClassLocations, CourseType searchCourseType, boolean ignoreFull, boolean ignoreConflict,
                                                   boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        pageIndex++;
        List<CourseSearchEntry> list=new ArrayList<>();
        int which=0,nums=0;
        if(searchCid!=null){
            which+=2;
        }
        if(searchName!=null){
            which+=1;
        }
        Connection con=null;
        int ind=1;
        try{
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta=con.prepareStatement (sta_searchCourse[which]);
            PreparedStatement sta_queryMajorCompulsory=con.prepareStatement (ImplementedStudentService.sta_queryMajorCompulsory),
                    sta_queryMajorElective=con.prepareStatement (ImplementedStudentService.sta_queryMajorElective),
                    sta_queryNotInMajor=con.prepareStatement (ImplementedStudentService.sta_queryNotInMajor),
                    sta_queryCourseInMajor=con.prepareStatement (ImplementedStudentService.sta_queryCourseInMajor),
                    sta_queryHasPass=con.prepareStatement (ImplementedStudentService.sta_queryHasPass),sta_queryStudentSelectedCourseInSemester=con.prepareStatement (ImplementedStudentService.sta_queryStudentSelectedCourseInSemester)
                    ;



            sta.setInt(ind,semesterId);
            ind++;
            int i,j,k;
            if(((which>>1)&1)==1){
                sta.setString(ind,searchCid);
                ind++;
            }
            if((which&1)==1){
                sta.setString(ind,searchName);
                ind++;
            }
            ResultSet resultSet=sta.executeQuery(),courseTypeResult;
            resultSet.setFetchSize(1);
            resultSet=sta.executeQuery();
            HashSet<String> majorCourse=new HashSet<>();
            switch (searchCourseType){
                case MAJOR_COMPULSORY:
                    sta_queryMajorCompulsory.setInt(1,studentId);
                    courseTypeResult=sta_queryMajorCompulsory.executeQuery();
                    while (courseTypeResult.next()){
                        majorCourse.add(courseTypeResult.getString(1));
                    }
                    break;
                case MAJOR_ELECTIVE:
                    sta_queryMajorElective.setInt(1,studentId);
                    courseTypeResult=sta_queryMajorElective.executeQuery();
                    while (courseTypeResult.next()){
                        majorCourse.add(courseTypeResult.getString(1));
                    }
                    break;
                case PUBLIC:
                    courseTypeResult=sta_queryNotInMajor.executeQuery();
                    while (courseTypeResult.next()){
                        majorCourse.add(courseTypeResult.getString(1));
                    }
                    break;
                case CROSS_MAJOR:
                    sta_queryCourseInMajor.setInt(1,studentId);
                    courseTypeResult=sta_queryCourseInMajor.executeQuery();
                    while (courseTypeResult.next()){
                        majorCourse.add(courseTypeResult.getString(1));
                    }
                    courseTypeResult=sta_queryNotInMajor.executeQuery();
                    while (courseTypeResult.next()){
                        majorCourse.add(courseTypeResult.getString(1));
                    }
                    break;
            }
            HashSet<String> passed=new HashSet<>();
//            c.id,c.name,c.credit,c.classHour,c.courseGrading,c2.id,c2.name,c2.totalCapacity,c2.leftCapacity,c3.id,c3.weekList,c3.classBegin,c3.classEnd,c3.location,a.id,a.fullName
            if(ignorePassed||ignoreMissingPrerequisites){
                sta_queryHasPass.setInt(1,studentId);
                ResultSet hasPass=sta_queryHasPass.executeQuery();
                while (hasPass.next()){
                    passed.add(hasPass.getString(1));
                }
            }
            List<JudgeConflictEntry> conflictEntries=new LinkedList<>();
            List<String> conflictEntryNames=new LinkedList<>();
            List<String> conflictIds=new LinkedList<>();
            HashSet<String> hasSelectedCourseId=new HashSet<>();
            sta_queryStudentSelectedCourseInSemester.setInt(1,studentId);
            sta_queryStudentSelectedCourseInSemester.setInt(2,semesterId);
            ResultSet conflict=sta_queryStudentSelectedCourseInSemester.executeQuery();
            while (conflict.next()){
//                    class.weeklist,class.dayofweek,class.classbegin,class.classend,d(course).name,c.name,c.id
                JudgeConflictEntry here=new JudgeConflictEntry();
                if(conflict.getObject(4)!=null){
                    here.weekList=(Short[]) conflict.getArray(1).getArray();
                    here.dayOfWeek=conflict.getInt(2);
                    here.begin=conflict.getShort(3);
                    here.end=conflict.getShort(4);
                    conflictEntries.add(here);
                }else {
                    conflictEntries.add(null);
                }
                conflictEntryNames.add(String.format("%s[%s]",conflict.getString(5),conflict.getString(6)));
                conflictIds.add(conflict.getString(7));
                hasSelectedCourseId.add(conflict.getString(7));
            }
            int useFulNum=0;
            int lastSectionId=-1;
            CourseSearchEntry here=null;
            boolean isWrong=false;
            HashSet<String> hasAdd=new HashSet<>();
//            @Nullable String searchCid, @Nullable String searchName,
//            @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime,
//            @Nullable List< String > searchClassLocations, CourseType searchCourseType
            boolean satisfySearchDayOfWeek=false,satisfySearchClassTime=false,satisfySearchClassLocations=false,satisfySearchCourseType=false,satisfyInstructor=false,satisfyTotal=false;
            while (resultSet.next()){
                //           1 c.id,2 c.name,3 c.credit,4 c.classHour,5 c.courseGrading,6 c2.id,7 c2.name,8 c2.totalCapacity,9 c2.leftCapacity,10 c3.id
//           ,11 c3.weekList,12 c3.classBegin,13 c3.classEnd,14 c3.location,15 a.id,16 a.fullName,17 c.prerequisite,18 c3.dayofweek, 19 a.firstName, 20 a.lastName (c is course, c2 is courseSection, c3 is courseSectionCLass,
                if(resultSet.getInt(6)!=lastSectionId){
                    if(here!=null){
                        if(!isWrong){
//                            list.add(here);
                            if(satisfyTotal&&satisfySearchCourseType){
                                useFulNum++;
                                if(useFulNum>pageIndex*pageSize){
                                    list.forEach(l->{
                                        l.conflictCourseNames.sort(Comparator.naturalOrder());
                                    });
                                    con.commit();
                                    con.close();

                                    return list;
                                }
                                if(useFulNum>(pageIndex-1)*pageSize){
                                    list.add(here);
                                }
                            }
                        }
                    }
                    hasAdd.clear();
                    isWrong=false;
                    here=new CourseSearchEntry();
                    Course course=new Course();
                    CourseSection section=new CourseSection();
                    Set<CourseSectionClass> classes=new HashSet<>();
                    here.course=course;
                    here.section=section;
                    here.sectionClasses=classes;
                    here.conflictCourseNames=new ArrayList<>();
                    course.id=resultSet.getString(1);
                    course.name=resultSet.getString(2);
                    course.credit=resultSet.getInt(3);
                    course.classHour=resultSet.getInt(4);
                    course.grading=resultSet.getInt(5)==0? Course.CourseGrading.PASS_OR_FAIL:Course.CourseGrading.HUNDRED_MARK_SCORE;
                    section.id=resultSet.getInt(6);
                    section.name=resultSet.getString(7);
                    section.totalCapacity=resultSet.getInt(8);
                    section.leftCapacity=resultSet.getInt(9);
                    lastSectionId=section.id;
                    satisfyTotal=false;
                    satisfySearchDayOfWeek=false;
                    satisfySearchClassTime=false;
                    satisfySearchClassLocations=false;
                    satisfySearchCourseType=false;
                    satisfyInstructor=false;
                    if(!satisfySearchCourseType){
                        switch (searchCourseType){
                            case ALL:
                                satisfySearchCourseType=true;
                                break;
                            case MAJOR_COMPULSORY:
                                if(majorCourse.contains(here.course.id)){
                                    satisfySearchCourseType=true;
                                }
                                break;
                            case MAJOR_ELECTIVE:
                                if(majorCourse.contains(here.course.id)){
                                    satisfySearchCourseType=true;
                                }
                                break;
                            case CROSS_MAJOR:
                                if(!majorCourse.contains(here.course.id)){
                                    satisfySearchCourseType=true;
                                }
                                break;
                            case PUBLIC:
                                if(majorCourse.contains(here.course.id)){
                                    satisfySearchCourseType=true;
                                }
                                break;
                        }
                    }
                }
                satisfySearchDayOfWeek=false;satisfySearchClassTime=false;satisfySearchClassLocations=false;satisfyInstructor=false;
                if(isWrong){
                    continue;
                }
                if(!satisfySearchCourseType){
                    continue;
                }
                JudgeConflictEntry nowClassEntry=new JudgeConflictEntry();
                CourseSectionClass sectionClass=null;
                if(resultSet.getObject(10)!=null){
                    sectionClass=new CourseSectionClass();
                    sectionClass.id=resultSet.getInt(10);
                    sectionClass.weekList=Arrays.stream(nowClassEntry.weekList=(Short[])resultSet.getArray(11).getArray()).collect(Collectors.toCollection(HashSet::new));
                    sectionClass.classBegin=nowClassEntry.begin=resultSet.getShort(12);
                    sectionClass.classEnd=nowClassEntry.end=resultSet.getShort(13);
                    sectionClass.location=resultSet.getString(14);
                    sectionClass.dayOfWeek=DayOfWeek.SUNDAY.plus(nowClassEntry.dayOfWeek=resultSet.getInt(18));
                }
//                instructor(ind>15) wait for after judging set up
//                boolean ignoreFull, boolean ignoreConflict,
//                                                  boolean ignorePassed, boolean ignoreMissingPrerequisites,
                assert here != null;
                if(ignoreFull){
                    if(here.section.leftCapacity<=0){
                        isWrong=true;
                        continue;
                    }
                }
                Iterator<JudgeConflictEntry> iteEntry=conflictEntries.iterator();
                Iterator<String> iteName=conflictEntryNames.iterator();
                Iterator<String> iteIds=conflictIds.iterator();
//
                if(ignoreConflict){
                    if(sectionClass!=null){
                        while (iteEntry.hasNext()){
                            JudgeConflictEntry entryHere=iteEntry.next();
                            String name=iteName.next();
                            if(!hasAdd.contains(name)){
                                if(hasConflict(entryHere,nowClassEntry)){
                                    hasAdd.add(name);
                                    here.conflictCourseNames.add(name);
                                    isWrong=true;
                                    break;
                                }else if(hasSelectedCourseId.contains(here.course.id)){
                                    hasAdd.add(name);
                                    here.conflictCourseNames.add(name);
                                    isWrong=true;
                                    break;
                                }
                            }
                        }
                    }else {
                        while (iteName.hasNext()){
                            String name=iteName.next();
                            if(!hasAdd.contains(name)){
                                if(hasSelectedCourseId.contains(here.course.id)){
                                    hasAdd.add(name);
                                    here.conflictCourseNames.add(name);
                                    isWrong=true;
                                    break;
                                }
                            }
                        }
                    }
                    if(isWrong){
                        continue;
                    }
                }else {
                    if(sectionClass!=null){
                        while (iteEntry.hasNext()){
                            JudgeConflictEntry entryHere=iteEntry.next();
                            String name=iteName.next();
                            String id=iteIds.next();
                            if(!hasAdd.contains(name)){
                                if(hasConflict(entryHere,nowClassEntry)){
                                    hasAdd.add(name);
                                    here.conflictCourseNames.add(name);
                                }else if(id.equals(here.course.id)){
                                    hasAdd.add(name);
                                    here.conflictCourseNames.add(name);
                                }
                            }
                        }
                    }else {
                        while (iteEntry.hasNext()){
//                            JudgeConflictEntry entryHere=iteEntry.next();
                            String name=iteName.next();
                            String id=iteIds.next();
                            if(id.equals(here.course.id)){
                                hasAdd.add(name);
                                here.conflictCourseNames.add(name);
                            }
                        }
                    }
                }
//                if(studentId==11713284){
////                    System.out.println("wrong");
//                }
                if(ignorePassed){
                    if(passed.contains(here.course.id)){
                        isWrong=true;
                        continue;
                    }
                }
                if(ignoreMissingPrerequisites){
                    JudgePrerequisite judgePrerequisite=new JudgePrerequisite(passed);
                    String str=resultSet.getString(17);
                    if((str!=null)&&(!PrerequisiteAndString.initAndSolve(resultSet.getString(17)).when(judgePrerequisite))){
                        isWrong=true;
                        continue;
                    }
                }
//                @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime,
//            @Nullable List< String > searchClassLocations, CourseType searchCourseType
                if(!satisfyTotal){
                    if((searchDayOfWeek!=null)&&(searchClassTime!=null)){
                        if(searchDayOfWeek.equals(sectionClass.dayOfWeek) &&(sectionClass.classBegin<=searchClassTime)&&(sectionClass.classEnd>=searchClassTime)){
                            satisfySearchDayOfWeek=true;
                            satisfySearchClassTime=true;
                        }
                    }else{
                        if((searchDayOfWeek==null)||(searchDayOfWeek.equals(sectionClass.dayOfWeek) )){
                            satisfySearchDayOfWeek=true;
                        }
                        if((searchClassTime==null)||((sectionClass.classBegin<=searchClassTime)&&(sectionClass.classEnd>=searchClassTime))){
                            satisfySearchClassTime=true;
                        }
                    }
                    if((searchClassLocations==null)||(satisfySearchClassLocations)){
                        satisfySearchClassLocations=true;
                    }else {
                        Iterator<String> ite=searchClassLocations.iterator();
                        while (ite.hasNext()){
                            if(sectionClass.location.contains(ite.next())){
                                satisfySearchClassLocations=true;
                                break;
                            }
                        }
                    }
                    if((searchInstructor==null)||containsInstructor(resultSet.getString(19),resultSet.getString(20),searchInstructor)){
                        satisfyInstructor=true;
                    }
//                15 a.id,16 a.fullName,17 c.prerequisite,18 c3.dayofweek (c is course, c2 is courseSection, c3 is courseSectionCLass,

                    if(satisfyInstructor&&satisfySearchClassLocations&&satisfySearchClassTime&&satisfySearchDayOfWeek){
                        satisfyTotal=true;
                    }
                }
                Instructor instructor=new Instructor();
                sectionClass.instructor=instructor;
                instructor.fullName=resultSet.getString(16);
                instructor.id=resultSet.getInt(15);
                here.sectionClasses.add(sectionClass);
            }
            if(!isWrong){
//                            list.add(here);
                if(satisfyTotal&&satisfySearchCourseType){
                    useFulNum++;
                    if(useFulNum>pageIndex*pageSize){
                        list.forEach(l->{
                            l.conflictCourseNames.sort(Comparator.naturalOrder());
                        });
                        con.commit();
                        con.close();
                        return list;
                    }
                    if(useFulNum>(pageIndex-1)*pageSize){
                        list.add(here);
                    }
                }
            }
            list.forEach(l->{
                l.conflictCourseNames.sort(Comparator.naturalOrder());
            });
            con.commit();
            con.close();
            return list;
        }catch (SQLException e){
            e.printStackTrace();
        }
        list.forEach(l->{
            l.conflictCourseNames.sort(Comparator.naturalOrder());
        });
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return list;
    }

    @Override
    public  EnrollResult enrollCourse(int studentId, int sectionId) {
        Connection con=null;
        try {
//            sta_enroll.setInt(1,studentId);
//            sta_enroll.setInt(2,sectionId);
//            sta_enroll.setInt(3,-3);
//            if(sta_enroll.executeUpdate()==1){
//                sta_subCapacity.setInt(1,sectionId);
//                sta_subCapacity.executeUpdate();
//                return EnrollResult.SUCCESS;
//            }
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryCourseAndSection=con.prepareStatement (ImplementedStudentService.sta_queryCourseAndSection),
                    sta_queryCourseSection=con.prepareStatement (ImplementedStudentService.sta_queryCourseSection),
                    sta_enroll=con.prepareStatement (ImplementedStudentService.sta_enroll),sta_subCapacity=con.prepareStatement (ImplementedStudentService.sta_subCapacity);
            sta_queryCourseAndSection.setInt(1,sectionId);
            ResultSet courseInfo=sta_queryCourseAndSection.executeQuery();
            if(courseInfo.next()){
//                a.leftcapacity,c.prerequisite,a.semesterid,c.name,c2.weeklist,c2.classbegin,c2.classend,c2.dayofweek,c.id
                int left=courseInfo.getInt(1);
                String pre=courseInfo.getString(2);
                int semesterId=courseInfo.getInt(3);
                String courseName=courseInfo.getString(4);
                String courseId=courseInfo.getString(9);
                List<JudgeConflictEntry> classes=new LinkedList<>();
                JudgeConflictEntry here=null;
                //                short begin1,begin2=-1,end1,end2=-1;
//                int day1=courseInfo.getInt(8),day2=-1;
//                a.leftcapacity,c.prerequisite,a.semesterid,c.name,5 c2.weeklist,c2.classbegin,c2.classend
                //                weekList1=(Short[]) courseInfo.getArray(5).getArray();
                if(courseInfo.getObject(6)!=null){
                    here=new JudgeConflictEntry();
                    here.weekList=(Short[]) courseInfo.getArray(5).getArray();
                    here.begin=courseInfo.getShort(6);
                    here.end=courseInfo.getShort(7);
                    here.dayOfWeek=courseInfo.getInt(8);
                    classes.add(here);
                }
//                begin1=courseInfo.getShort(6);
//                end1=courseInfo.getShort(7);
                while(courseInfo.next()){
                    here=new JudgeConflictEntry();
                    here.weekList=(Short[]) courseInfo.getArray(5).getArray();
                    here.begin=courseInfo.getShort(6);
                    here.end=courseInfo.getShort(7);
                    here.dayOfWeek=courseInfo.getInt(8);
                    classes.add(here);
                }
//                1 a.grade,2 b.id,3 name,4 courseid,5 totalcapacity,6 leftcapacity,7 semesterid,8 c.weeklist,9 c.dayofweek,10 c.classbegin,11 c.classend
                sta_queryCourseSection.setInt(1,studentId);
                ResultSet allSection=sta_queryCourseSection.executeQuery();
                HashSet<String> passed=new HashSet<>();
                int i,j,k,grade,whichSemester;
                HashSet<String> hasEnrolled=new HashSet<>();
                List<JudgeConflictEntry> list=new LinkedList<>();
                while (allSection.next()){
                    if(allSection.getInt(2)==sectionId){
                        con.commit();
                        con.close();
                        return EnrollResult.ALREADY_ENROLLED;
                    }else{
                        grade=allSection.getInt(1);
                        if((grade>=60)||(grade==-1)){
                            String name=allSection.getString(4);
                            if(name.equals(courseId)){
                                con.commit();
                                con.close();
                                return EnrollResult.ALREADY_PASSED;
                            }
                            passed.add(name);
                        }

                        if(allSection.getInt(7)==semesterId){
                            if(allSection.getObject(9)!=null){
                                JudgeConflictEntry entry=new JudgeConflictEntry();
                                entry.weekList=(Short[]) allSection.getArray(8).getArray();
                                entry.dayOfWeek=allSection.getInt(9);
                                entry.begin=allSection.getShort(10);
                                entry.end=allSection.getShort(11);
                                list.add(entry);
                            }
                            hasEnrolled.add(allSection.getString(4));
                        }
                    }
                }
                if(pre!=null){
                    Prerequisite prerequisite=PrerequisiteAndString.initAndSolve(pre);
                    if(!prerequisite.when(new JudgePrerequisite(passed))){
                        con.commit();
                        con.close();
                        return EnrollResult.PREREQUISITES_NOT_FULFILLED;
                    }
                }
                Iterator<JudgeConflictEntry> ite=list.iterator();
                if(hasEnrolled.contains(courseId)){
                    con.commit();
                    con.close();
                    return EnrollResult.COURSE_CONFLICT_FOUND;
                }
                while (ite.hasNext()){
                    here=ite.next();
                    JudgeConflictEntry courseEntry;
                    Iterator<JudgeConflictEntry> ite2=classes.iterator();
                    i=0;j=0;
                    while (ite2.hasNext()){
                        courseEntry=ite2.next();
                        if(hasSame(here.weekList,courseEntry.weekList)){
                            if(here.dayOfWeek==courseEntry.dayOfWeek){
                                if(here.begin<=courseEntry.end&&here.end>=courseEntry.begin){
                                    con.commit();
                                    con.close();
                                    return EnrollResult.COURSE_CONFLICT_FOUND;
                                }
                            }
                        }
                    }
                }
                if(left==0){
                    con.commit();
                    con.close();
                    return EnrollResult.COURSE_IS_FULL;
                }
                sta_enroll.setInt(1,studentId);
                sta_enroll.setInt(2,sectionId);
                sta_enroll.setInt(3,-3);
                if(sta_enroll.executeUpdate()==1){
                    sta_subCapacity.setInt(1,sectionId);
                    sta_subCapacity.executeUpdate();
                    con.commit();
                    con.close();
                    return EnrollResult.SUCCESS;
                }else {
                    con.commit();
                    con.close();
                    return EnrollResult.UNKNOWN_ERROR;
                }
            }else {
                con.commit();
                con.close();
                return EnrollResult.COURSE_NOT_FOUND;
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
        return EnrollResult.UNKNOWN_ERROR;
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException {
        try {
            Connection connection=SQLDataSource.getInstance().getSQLConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            PreparedStatement sta_queryGrade=connection.prepareStatement("select s.grade  from studenttocoursesection s where studentid=? and coursesectionid=?");
            int grade=0;
            sta_queryGrade.setInt(1,studentId);
            sta_queryGrade.setInt(2,sectionId);
            sta_queryGrade.executeQuery();
            ResultSet resultSet= sta_queryGrade.getResultSet();
            if(resultSet.next()){
                grade=resultSet.getInt(1);
            }else {
                connection.rollback();
                connection.close();
                throw new EntityNotFoundException();
            }
            if(grade>-3){
                connection.rollback();
                connection.close();
                throw new IllegalStateException();
            }
            PreparedStatement sta_dropCourse=connection.prepareStatement("delete from studenttocoursesection where studentId=? and coursesectionid=?");
            sta_dropCourse.setInt(1,studentId);
            sta_dropCourse.setInt(2,sectionId);
            int a=sta_dropCourse.executeUpdate();
            if(a==0){
                connection.rollback();
//                connection.setAutoCommit(true);
                connection.close();
                throw new IllegalStateException();
            }
            PreparedStatement sta_addCapacity=connection.prepareStatement("update coursesection set leftcapacity=leftcapacity+1 where id=?");
            sta_addCapacity.setInt(1,sectionId);
            sta_addCapacity.execute();
            connection.commit();
//            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public  void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        Connection connection=null;
        try {
            connection=SQLDataSource.getInstance().getSQLConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            PreparedStatement sta_enroll=connection.prepareStatement("insert into studenttocoursesection values (?,?,?)");
            sta_enroll.setInt(1,studentId);
            sta_enroll.setInt(2,sectionId);
            if(grade==null){
                sta_enroll.setInt(3,-3);
            }else {
                if(grade instanceof HundredMarkGrade){
                    sta_enroll.setInt(3,((HundredMarkGrade) grade).mark);
                }else {
                    if((PassOrFailGrade)grade==PassOrFailGrade.PASS){
                        sta_enroll.setInt(3,-1);
                    }else {
                        sta_enroll.setInt(3,-2);
                    }
                }
            }
            sta_enroll.execute();
            connection.commit();
//            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.commit ();
                connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            throw new IntegrityViolationException();
        }
    }

    @Override
    public  void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        Connection con=null;

        try {

            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_updGrade=con.prepareStatement (ImplementedStudentService.sta_updGrade);
            sta_updGrade.setInt(2,studentId);
            sta_updGrade.setInt(3,sectionId);
            if(grade instanceof HundredMarkGrade){
                sta_updGrade.setInt(1,((HundredMarkGrade) grade).mark);
            }else {
                sta_updGrade.setInt(1,grade==PassOrFailGrade.PASS?-1:-2);
            }
            sta_updGrade.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public  Map< Course, Grade > getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId) {
//        c.grade,d.id,d.name,credit, classhour, coursegrading,c.beginDate
        Connection con=null;
        Map<Course,Grade> map=new HashMap<>();
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            ResultSet resultSet;
            PreparedStatement sta_courseAndGrades=con.prepareStatement (ImplementedStudentService.sta_courseAndGrades),
                    sta_courseAndGradesWithSemester=con.prepareStatement (ImplementedStudentService.sta_courseAndGradesWithSemester);
            if(semesterId==null){
                sta_courseAndGrades.setInt(1,studentId);
                resultSet=sta_courseAndGrades.executeQuery();
            }else {
                sta_courseAndGradesWithSemester.setInt(1,studentId);
                sta_courseAndGradesWithSemester.setInt(2,semesterId);
                resultSet=sta_courseAndGradesWithSemester.executeQuery();
            }
            Course cHere;
            Grade gHere;
            int grade;
            while (resultSet.next()){
                cHere=new Course();
                grade=resultSet.getInt(1);
                if(grade<0){
                    if(grade==-1){
                        gHere=PassOrFailGrade.PASS;
                    }else if(grade==-2){
                        gHere=PassOrFailGrade.FAIL;
                    }else {
                        gHere=null;
                    }
                    cHere.grading= Course.CourseGrading.PASS_OR_FAIL;
                }else {
                    gHere=new HundredMarkGrade((short) grade);
                    cHere.grading= Course.CourseGrading.HUNDRED_MARK_SCORE;
                }
                cHere.id=resultSet.getString(2);
                cHere.name=resultSet.getString(3);
                cHere.credit=resultSet.getInt(4);
                cHere.classHour=resultSet.getInt(5);
                map.put(cHere,gHere);
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
        return map;
    }

    @Override
    public  CourseTable getCourseTable(int studentId, Date date) {
//       1 c2.dayofweek,2 c2.weeklist,3 c.id,4 a2.id,5 a2.fullname,6 c2.classbegin,7 c2.classend,8 c2.location,9 s.begindate,10 s.enddate,11 c.name, 12 b.name
        CourseTable courseTable=new CourseTable();
        Connection con=null;
        courseTable.table=new HashMap<>();
        Set< CourseTable.CourseTableEntry >[] sets=new Set[8];
        int i,j,k,day=2;
        for(i=1;i<=7;i++){
            sets[i]= new HashSet<>();
            courseTable.table.put(DayOfWeek.SUNDAY.plus(i),sets[i]);
        }
        try {

            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryCourseTable=con.prepareStatement (ImplementedStudentService.sta_queryCourseTable);
            sta_queryCourseTable.setInt(1,studentId);
            ResultSet resultSet=sta_queryCourseTable.executeQuery();
            LocalDate begin,end;
            LocalDate change=date.toLocalDate();
            if(change.getDayOfWeek().equals(DayOfWeek.MONDAY)){
                change=change.plusDays(1);
            }
            if(change.getDayOfWeek().equals(DayOfWeek.SUNDAY)){
                change=change.minusDays(1);
            }
//            switch (change.getDayOfWeek()){
//                case MONDAY:change=change.plusDays(1);
//                    break;
//                case WEDNESDAY:change=change.minusDays(1);
//                    break;
//                case THURSDAY:change=change.minusDays(2);
//                    break;
//                case FRIDAY:change=change.minusDays(3);
//                    break;
//                case SATURDAY:change=change.minusDays(4);
//                    break;
//                case SUNDAY:change=change.minusDays(5);
//                    break;
//            }
            int week=0;
            Short[] weekList;
            CourseTable.CourseTableEntry here=null;
            Instructor instructor=null;
            while (resultSet.next()){
                //       1 c2.dayofweek,2 c2.weeklist,3 c.id,4 a2.id,5 a2.fullname,6 c2.classbegin,7 c2.classend,8 c2.location,9 s.begindate,10 s.enddate,11 c.name, 12 b.name c2 is class,
//                b is section, c is course
                day=resultSet.getInt(1);
                begin=resultSet.getDate(9).toLocalDate();
                end=resultSet.getDate(10).toLocalDate();
                if(change.isBefore(end)&&change.isAfter(begin)){
                    week=(int)(change.toEpochDay()-begin.toEpochDay())/7+1;
                    weekList= (Short[]) resultSet.getArray(2).getArray();
                    boolean inWeek=false;
                    for(i=0;i<weekList.length;i++){
                        if(weekList[i]==week){
                            inWeek=true;
                            break;
                        }
                    }
                    if(inWeek){
                        here=new CourseTable.CourseTableEntry();
                        here.classBegin=resultSet.getShort(6);
                        here.classEnd=resultSet.getShort(7);
                        here.courseFullName=String.format("%s[%s]",resultSet.getString(11),resultSet.getString(12));
                        here.instructor=instructor=new Instructor();
                        instructor.id=resultSet.getInt(4);
                        instructor.fullName=resultSet.getString(5);
                        here.location=resultSet.getString(8);
                        sets[day].add(here);
                    }
                }
//                list[day].add();
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
        return courseTable;
    }

    @Override
    public  boolean passedPrerequisitesForCourse(int studentId, String courseId) {
        Connection con=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_queryHasPass=con.prepareStatement (ImplementedStudentService.sta_queryHasPass),
                    sta_queryPre=con.prepareStatement (ImplementedStudentService.sta_queryPre);

            sta_queryHasPass.setInt(1,studentId);
            String pre = null;
            sta_queryPre.setString(1,courseId);
            ResultSet resultSet=sta_queryPre.executeQuery();
            if(resultSet.next()){
                pre=resultSet.getString(1);
            }
            if(pre==null){
                con.commit();
                con.close();
                return true;
            }
            HashSet<String> passed=new HashSet<>();
            resultSet=sta_queryHasPass.executeQuery();
            while (resultSet.next()){
                passed.add(resultSet.getString(1));
            }
            Prerequisite need= PrerequisiteAndString.initAndSolve(pre);
            con.commit();
            con.close();
            return need.when(new JudgePrerequisite(passed));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            con.commit();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return false;
    }

    @Override
    public  Major getStudentMajor(int studentId) {
        Connection con=null;
        Major major=null;
        try {
            con=SQLDataSource.getInstance().getSQLConnection();
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            con.setAutoCommit(false);
            PreparedStatement sta_studentMajor=con.prepareStatement (ImplementedStudentService.sta_studentMajor);
            sta_studentMajor.setInt(1,studentId);
            ResultSet resultSet=sta_studentMajor.executeQuery();
            if(resultSet.next()){
                major=new Major();
                major.id=resultSet.getInt(1);
                major.name=resultSet.getString(2);
                Department d=new Department();
                major.department=d;
                d.id=resultSet.getInt(3);
                d.name=resultSet.getString(4);
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
        if(major==null){
            throw new EntityNotFoundException();
        }
        return major;
    }
}
