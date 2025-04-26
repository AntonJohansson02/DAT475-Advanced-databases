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

def main():
    # Set up graph
    g, ns = setup_graph()
    
    # Process different data sources
    g = process_course_data(g, ns)
    g = process_student_data(g, ns)
    
    # Serialize the graph
    g.serialize(destination="combined_data.ttl", format="turtle")
    print("combined_data.ttl has been created!")

if __name__ == "__main__":
    main()