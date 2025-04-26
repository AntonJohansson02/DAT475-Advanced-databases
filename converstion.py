import pandas as pd
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD

def setup_graph():
    """Set up the RDF graph and namespace"""
    g = Graph()
    ns = Namespace("http://www.semanticweb.org/anton/ontologies/2025/3/Assignment2/")
    g.parse("Assignment2.ttl", format="turtle")
    return g, ns

def process_course_data(g, ns):
    """Process course instance and planning data"""
    # Load CSV files
    instances_df = pd.read_csv("Course_Instances.csv")
    planning_df = pd.read_csv("Course_plannings.csv")
    
    # Merge the DataFrames
    merged_df = pd.merge(instances_df, planning_df, left_on="Instance_id", right_on="Course", how="inner")
    
    # Iterate through the merged DataFrame and add RDF triples
    for _, row in merged_df.iterrows():
        ci_uri = ns[f"courseInstance_{row['Instance_id']}"]
        g.add((ci_uri, RDF.type, ns.CourseInstance))
        g.add((ci_uri, ns.instanceId, Literal(row['Instance_id'], datatype=XSD.string)))
        g.add((ci_uri, ns.studyPeriod, Literal(int(float(row['Study period'])), datatype=XSD.int)))
        g.add((ci_uri, ns.studyYear, Literal(int(row['Academic year'].split("-")[0]), datatype=XSD.int)))
        g.add((ci_uri, ns.planningNumStudents, Literal(int(row['Planned number of Students']), datatype=XSD.int)))
        g.add((ci_uri, ns.seniorHours, Literal(int(row['Senior Hours']), datatype=XSD.int)))
        g.add((ci_uri, ns.assistantHours, Literal(int(row['Assistant Hours']), datatype=XSD.int)))

        # Link to Course
        course_uri = ns[f"course_{row['Course code']}"]
        g.add((ci_uri, ns.cInstanceOf, course_uri))
        g.add((course_uri, RDF.type, ns.Course))
        g.add((course_uri, ns.courseCode, Literal(str(row['Course code']), datatype=XSD.string)))

        # Link to examiner (as a SeniorTeacher)
        teacher_uri = ns[f"teacher_{row['Examiner']}"]
        g.add((teacher_uri, RDF.type, ns.SeniorTeacher))
        g.add((teacher_uri, ns.teacherId, Literal(row['Examiner'], datatype=XSD.string)))
        g.add((ci_uri, ns.examinedBy, teacher_uri))
    
    return g

def process_student_data(g, ns):
    """Process student data from Students.csv"""
    # Load the CSV file
    students_df = pd.read_csv("Students.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in students_df.iterrows():
        # Create student URI
        student_uri = ns[f"person_{row['Student id']}"]
        
        # Add common person properties
        g.add((student_uri, RDF.type, ns.Person))
        g.add((student_uri, ns.personId, Literal(row['Student id'], datatype=XSD.string)))
        g.add((student_uri, ns.name, Literal(row['Student name'], datatype=XSD.string)))
        
        # Check if this is a TA or regular student based on name
        if row['Student name'].startswith('TA '):
            g.add((student_uri, RDF.type, ns.TeachingAssistant))
        else:
            g.add((student_uri, RDF.type, ns.Student))
        
        # Add program information
        program_uri = ns[f"program_{row['Programme']}"]
        g.add((program_uri, RDF.type, ns.Program))
        g.add((program_uri, ns.programCode, Literal(row['Programme'], datatype=XSD.string)))
        g.add((student_uri, ns.enrolledIn, program_uri))
        
        # Add year started and graduation status
        g.add((student_uri, ns.yearStarted, Literal(int(row['Year']), datatype=XSD.int)))
        g.add((student_uri, ns.graduated, Literal(row['Graduated'] == 'True', datatype=XSD.boolean)))
    
    return g

def process_teacher_data(g, ns):
    """Process senior teacher data from Senior_Teachers.csv"""
    # Load the CSV file
    teachers_df = pd.read_csv("Senior_Teachers.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in teachers_df.iterrows():
        # Create teacher URI
        teacher_uri = ns[f"teacher_{row['Teacher id']}"]
        
        # Add teacher properties
        g.add((teacher_uri, RDF.type, ns.SeniorTeacher))
        g.add((teacher_uri, RDF.type, ns.Person))
        g.add((teacher_uri, ns.teacherId, Literal(row['Teacher id'], datatype=XSD.string)))
        g.add((teacher_uri, ns.name, Literal(row['Teacher name'], datatype=XSD.string)))
        
        # Add department information
        dept_uri = ns[f"department_{row['Department name']}"]
        g.add((dept_uri, RDF.type, ns.Department))
        g.add((dept_uri, ns.departmentCode, Literal(row['Department name'], datatype=XSD.string)))
        g.add((teacher_uri, ns.belongsToDepartment, dept_uri))
        
        # Add division information
        division_uri = ns[f"division_{row['Division name']}"]
        g.add((division_uri, RDF.type, ns.Division))
        g.add((division_uri, ns.divisionCode, Literal(row['Division name'], datatype=XSD.string)))
        g.add((teacher_uri, ns.belongsToDivision, division_uri))
        
        # Link division to department
        g.add((division_uri, ns.partOfDepartment, dept_uri))
    
    return g

def process_ta_data(g, ns):
    """Process teaching assistant data from Teaching_Assistants.csv"""
    # Load the CSV file
    ta_df = pd.read_csv("Teaching_Assistants.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in ta_df.iterrows():
        # Create TA URI
        ta_uri = ns[f"ta_{row['Teacher id']}"]
        
        # Add TA properties
        g.add((ta_uri, RDF.type, ns.TeachingAssistant))
        g.add((ta_uri, RDF.type, ns.Person))
        g.add((ta_uri, ns.teacherId, Literal(row['Teacher id'], datatype=XSD.string)))
        g.add((ta_uri, ns.name, Literal(row['Teacher name'], datatype=XSD.string)))
        
        # Add department information
        dept_uri = ns[f"department_{row['Department name']}"]
        g.add((dept_uri, RDF.type, ns.Department))
        g.add((dept_uri, ns.departmentCode, Literal(row['Department name'], datatype=XSD.string)))
        g.add((ta_uri, ns.belongsToDepartment, dept_uri))
        
        # Add division information
        division_uri = ns[f"division_{row['Division name']}"]
        g.add((division_uri, RDF.type, ns.Division))
        g.add((division_uri, ns.divisionCode, Literal(row['Division name'], datatype=XSD.string)))
        g.add((ta_uri, ns.belongsToDivision, division_uri))
        
        # Link division to department
        g.add((division_uri, ns.partOfDepartment, dept_uri))
    
    return g

def process_reported_hours(g, ns):
    """Process teaching hours from Reported_Hours.csv"""
    # Load the CSV file
    hours_df = pd.read_csv("Reported_Hours.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in hours_df.iterrows():
        # Get course and teacher URIs
        course_uri = ns[f"course_{row['Course code']}"]
        
        # Determine if it's a senior teacher or TA based on ID format 
        # (assuming both types can report hours)
        teacher_id = row['Teacher Id']
        if len(teacher_id) > 10:  # Personal number format for senior teachers
            teacher_uri = ns[f"teacher_{teacher_id}"]
        else:
            teacher_uri = ns[f"ta_{teacher_id}"]
        
        # Create a teaching assignment relationship
        assignment_uri = ns[f"teaching_{row['Course code']}_{teacher_id}"]
        g.add((assignment_uri, RDF.type, ns.TeachingAssignment))
        g.add((assignment_uri, ns.hasTeacher, teacher_uri))
        g.add((assignment_uri, ns.hasCourse, course_uri))
        g.add((assignment_uri, ns.reportedHours, Literal(float(row['Hours']), datatype=XSD.float)))
        
        # Add direct links for easier querying
        g.add((teacher_uri, ns.teachesIn, course_uri))
        g.add((teacher_uri, ns.hasReportedHours, Literal(float(row['Hours']), datatype=XSD.float)))
        g.add((course_uri, ns.hasTeachingContribution, assignment_uri))
    
    return g

def process_registrations(g, ns):
    """Process student registrations from Registrations.csv"""
    # Load the CSV file
    reg_df = pd.read_csv("Registrations.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in reg_df.iterrows():
        # Get course instance and student URIs
        course_instance_uri = ns[f"courseInstance_{row['Course Instance']}"]
        student_uri = ns[f"person_{row['Student id']}"]
        
        # Create registration URI
        registration_uri = ns[f"registration_{row['Course Instance']}_{row['Student id']}"]
        g.add((registration_uri, RDF.type, ns.Registration))
        
        # Link registration to student and course instance
        g.add((registration_uri, ns.hasStudent, student_uri))
        g.add((registration_uri, ns.hasCourseInstance, course_instance_uri))
        
        # Add status information
        g.add((registration_uri, ns.status, Literal(row['Status'], datatype=XSD.string)))
        
        # Add direct links for easier querying
        g.add((student_uri, ns.registeredFor, course_instance_uri))
        g.add((course_instance_uri, ns.hasRegistration, registration_uri))
        
        # Add grade if completed
        if row['Status'] == 'completed' and not pd.isna(row['Grade']):
            g.add((registration_uri, ns.grade, Literal(float(row['Grade']), datatype=XSD.float)))
    
    return g

def process_programmes(g, ns):
    """Process programme data from Programmes.csv"""
    # Load the CSV file
    prog_df = pd.read_csv("Programmes.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in prog_df.iterrows():
        # Create programme URI
        programme_uri = ns[f"program_{row['Programme code']}"]
        
        # Add programme properties
        g.add((programme_uri, RDF.type, ns.Program))
        g.add((programme_uri, ns.programName, Literal(row['Programme name'], datatype=XSD.string)))
        g.add((programme_uri, ns.programCode, Literal(row['Programme code'], datatype=XSD.string)))
        
        # Add department information
        dept_uri = ns[f"department_{row['Department name']}"]
        g.add((dept_uri, RDF.type, ns.Department))
        g.add((dept_uri, ns.departmentCode, Literal(row['Department name'], datatype=XSD.string)))
        g.add((programme_uri, ns.belongsToDepartment, dept_uri))
        
        # Add director information
        director_uri = ns[f"teacher_{row['Director']}"]
        g.add((director_uri, RDF.type, ns.SeniorTeacher))
        g.add((programme_uri, ns.hasDirector, director_uri))
        g.add((director_uri, ns.directorOf, programme_uri))
    
    return g

def process_programme_courses(g, ns):
    """Process programme course data from Programme_Courses.csv"""
    # Load the CSV file
    prog_courses_df = pd.read_csv("Programme_Courses.csv")
    
    # Iterate through the DataFrame and add RDF triples
    for _, row in prog_courses_df.iterrows():
        # Get URIs for the program and course
        program_uri = ns[f"program_{row['Programme code']}"]
        course_uri = ns[f"course_{row['Course']}"]
        
        # Create a ProgrammeCourse relationship
        pc_uri = ns[f"progcourse_{row['Programme code']}_{row['Course']}_{row['Academic Year']}_{row['Study Year']}"]
        g.add((pc_uri, RDF.type, ns.ProgrammeCourse))
        
        # Link to Program
        g.add((pc_uri, ns.hasProgram, program_uri))
        
        # Link to Course
        g.add((pc_uri, ns.hasCourse, course_uri))
        
        # Add course type
        g.add((pc_uri, ns.courseType, Literal(row['Course Type'], datatype=XSD.string)))
        
        # Add academic year
        academic_year = row['Academic Year']
        g.add((pc_uri, ns.academicYear, Literal(academic_year, datatype=XSD.string)))
        start_year = int(academic_year.split('-')[0])
        g.add((pc_uri, ns.academicStartYear, Literal(start_year, datatype=XSD.int)))
        
        # Add study year
        g.add((pc_uri, ns.studyYear, Literal(float(row['Study Year']), datatype=XSD.float)))
        
        # Add direct links for easier querying
        g.add((program_uri, ns.includesCourse, course_uri))
        g.add((course_uri, ns.includedInProgram, program_uri))
    
    return g

def main():
    # Set up graph
    g, ns = setup_graph()
    
    # Process different data sources
    g = process_course_data(g, ns)
    g = process_student_data(g, ns)
    g = process_teacher_data(g, ns)
    g = process_ta_data(g, ns)
    g = process_reported_hours(g, ns)
    g = process_registrations(g, ns)
    g = process_programmes(g, ns)
    g = process_programme_courses(g, ns)
    
    # Serialize the graph
    g.serialize(destination="combined_data.ttl", format="turtle")
    print("combined_data.ttl has been created!")

if __name__ == "__main__":
    main()