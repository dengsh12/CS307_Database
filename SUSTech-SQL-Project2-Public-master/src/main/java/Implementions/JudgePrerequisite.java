package Implementions;

import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;

import java.util.Iterator;
import java.util.Set;

public class JudgePrerequisite implements Prerequisite.Cases<Boolean> {
    Set<String> passed;
    JudgePrerequisite(Set<String> passed){
        this.passed=passed;
    }
    @Override
    public Boolean match(AndPrerequisite self) {
        Iterator<Prerequisite>ite=self.terms.iterator();
        while (ite.hasNext()){
            if(!ite.next().when(this)){
                return false;
            }
        }
        return true;
    }
    @Override
    public Boolean match(OrPrerequisite self) {
        Iterator<Prerequisite>ite=self.terms.iterator();
        while (ite.hasNext()){
            if(ite.next().when(this)){
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean match(CoursePrerequisite self) {
        return passed.contains(self.courseID);
    }
}
