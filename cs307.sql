create table department
(
    id   serial primary key,
    name varchar unique
);

create table semester
(
    id        serial primary key,
    name      varchar,
    beginDate date,
    endDate   date
);

create table major
(
    id           serial primary key,
    name         varchar,
    departmentId int references department (id) on delete cascade
);
create index idx_major_departmentId on major(departmentId);
create table allUser(
                        id int primary key ,
                        role int,--0 student, 1 instructor
                        fullName varchar,
                        enrolledDate date,
                        majorId      int references major (id) on delete cascade ,
                        firstName varchar,
                        lastName varchar
);
create index idx_alluser_majorId on allUser(majorId);
create table course
(
    id            varchar primary key,
    name          varchar,
    credit        int,
    classHour     int,
    courseGrading int,--0，pf,1,percentage
    prerequisite varchar
);

create table courseSection
(
    id            serial primary key,
    name          varchar,
    courseId      varchar references course (id) on delete cascade ,
    totalCapacity int,
    leftCapacity  int,
    semesterId    int references semester (id) on delete cascade
);
-- create index ind_courseSection_courseId on courseSection(courseId);

create index idx_courseSection_courseIdAndSemesterId on courseSection(courseId,semesterId);

create table courseSectionClass
(
    id              serial primary key,
    instructorId    int references allUser (id) on delete cascade ,
    dayOfWeek       int,
    weekList        smallint[],
    classBegin      smallint,
    classEnd        smallint,
    location        varchar,
    courseSectionId int references courseSection (id) on delete cascade
);

create index idx_courseSectionClass_courseSectionId on courseSectionClass(courseSectionId);
create index idx_courseSectionClass_instructorId on courseSectionClass(instructorId);

create table courseToMajor
(
    courseId varchar references course (id) on delete cascade ,
    majorId  int references major (id) on delete cascade ,
    relation int,--1 compulsory,0 Elective
    primary key (courseId, majorId)
);
create index inx_courseToMajor_majorId on courseToMajor(majorId);

create table studentToCourseSection
(
    studentId       int references allUser (id) on delete cascade ,
    courseSectionId int references courseSection (id) on delete cascade ,
    grade           int,
    primary key (studentId,courseSectionId)
);
create index idx_studentToCourseSection_courseSectionId on studentToCourseSection(courseSectionId);