package Implementions;
import cn.edu.sustech.cs307.factory.ServiceFactory;
import cn.edu.sustech.cs307.service.*;

public class ImplementedFactory extends ServiceFactory {
    public ImplementedFactory(){
        registerService(CourseService.class, new ImplementedCourseService());
        registerService(DepartmentService.class, new ImplementedDepartmentService());
        registerService(InstructorService.class, new ImplementedInstructorService());
        registerService(MajorService.class, new ImplementedMajorService());
        registerService(SemesterService.class, new ImplementedSemesterService());
        registerService(StudentService.class, new ImplementedStudentService());
        registerService(UserService.class, new ImplementedUserService());
    }
}
