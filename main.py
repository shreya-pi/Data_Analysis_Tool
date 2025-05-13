from entity_scripts.tables import TableComparer
from entity_scripts.views import ViewComparer
from entity_scripts.function import FunctionComparer
from entity_scripts.procedures import ProcedureComparer
from compare import Compare



class DataQualityTool:
    def __init__(self):
        self.table_comparer = TableComparer()
        self.view_comparer = ViewComparer()
        # self.comparer = Compare()

    def run(self, entity):
        if entity == 'table':
            print("Comparing tables...")
            # Compare all tables
            self.table_comparer.compare_all_tables()
        elif entity == 'view':
            print("Comparing views...")
            # Compare all views
            self.view_comparer.compare_all_views()
        elif entity == 'function':
            print("Comparing functions...")
            # Compare all functions
            function_comparer = FunctionComparer()
            function_comparer.compare_all_functions()
        elif entity == 'procedure':
            print("Comparing procedures...")
            # Compare all procedures
            procedure_comparer = ProcedureComparer()
            procedure_comparer.compare_all_procedures()
        else:
            print("Invalid entity type. Please choose 'table' or 'view'.")
            return



        # Additional functionality can be added here
if __name__ == "__main__":
 
    dq_tool = DataQualityTool()
    print("Welcome to the Data Quality Tool!")
    print("Choose an option:")
    print("1. Compare Tables")
    print("2. Compare Views")
    print("3. Compare Functions")
    print("4. Compare Procedures")

    case = input("Enter your choice: ")
    if case == '1':
        entity = 'table'
    elif case == '2':
        entity = 'view'
    elif case == '3':
        entity = 'function'
    elif case == '4':
        entity = 'procedure'
    else:
        print("Invalid choice. Please enter 1 or 2.")
        exit()

    dq_tool.run(entity)

