package Implementions;

import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class PrerequisiteAndString implements Prerequisite.Cases<String> {
    public static int[] bracket=new int[1000];
    static String str;
    public static int bracketNum;
    @Override
    public String match(AndPrerequisite self) {
        String[] children = self.terms.stream()
                .map(term -> term.when(this))
                .toArray(String[]::new);
        return '(' + String.join("&", children) + ')';
    }

    @Override
    public String match(OrPrerequisite self) {
        String[] children = self.terms.stream()
                .map(term -> term.when(this))
                .toArray(String[]::new);
        return '(' + String.join("|", children) + ')';
    }

    @Override
    public String match(CoursePrerequisite self) {
        return self.courseID;
    }

    public static Prerequisite initAndSolve(String str){
        synchronized (PrerequisiteAndString.class){
            bracketNum=0;
            PrerequisiteAndString.str=str;
            int i,j;
            Stack<Integer> stack=new Stack<>();
            j=0;
            for(i=0;i<str.length();i++){
                if(str.charAt(i)=='('){
                    stack.push(j++);
                }
                if(str.charAt(i)==')'){
                    bracket[stack.pop()]=i;
                }
            }
            int start=0,end=str.length()-1;
            while (str.charAt(start)=='('&&str.charAt(end)==')'&&bracket[bracketNum]==end){
                start++;
                end--;
                bracketNum++;
            }
            return toPrerequisite(start,end);
        }
    }
    public static Prerequisite toPrerequisite(int start,int end){

        int i,j,last=start,type=0;//type:0 normal, 1 and, 2 or
        List<Prerequisite> list=new ArrayList<>();
        CoursePrerequisite here;
        Prerequisite result;
        for(i=start;i<=end;i++){
            switch (str.charAt(i)){
                case '&':
                    type=1;
                    if(i>last){
                        here=new CoursePrerequisite(str.substring(last,i));
                        list.add(here);
                    }
                    last=i+1;
                    break;
                case '|':
                    type=2;
                    if(i>last){
                        here=new CoursePrerequisite(str.substring(last,i));
                        list.add(here);
                    }
                    last=i+1;
                    break;
                case '(':
                    last=bracket[bracketNum]+2;
                    bracketNum++;
                    list.add(toPrerequisite(i+1,last-3));
                    i=last-2;
            }
        }
        if(type==0){
            return new CoursePrerequisite(str.substring(last,end+1));
        }else {
            if(last<end){
                list.add(new CoursePrerequisite(str.substring(last,end+1)));
            }
            if(type==1){
                return new AndPrerequisite(list);
            }else {
                return new OrPrerequisite(list);
            }
        }
    }
}
