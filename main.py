%restart_python

import sys
import os

is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

if is_databricks:
    try:
        import IPython
        ipython = IPython.get_ipython()
        if ipython is not None:
            ipython.run_line_magic("pip", "install -r requirements.txt")
    except Exception as e:
            print(f"Err loading modules : {e}")

    try:
        root_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        root_path = os.getcwd()

    if root_path not in sys.path:
        sys.path.append(root_path)

def main():
    # Define main.py absolute path
    try:
        # Local
        root_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Databricks
        root_path = os.getcwd()
    
    # get arg from input (terminal)
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    
    from src.utils.db_factory import get_db_connector
    from src.pipelines.historical_pipeline import run_historical_pipeline
    # from src.pipelines.incremental_pipeline import run_incremental_pipeline
    
    try:
        # get right db depending on environement (local or databricks) from db factory
        db_connector = get_db_connector()
        
        if mode == "historical":
            print("Start importing historical data")
            run_historical_pipeline(db_connector)
            
        elif mode == "incremental":
            pass
            # run_incremental_pipeline(db_connector)
            
        else:
            sys.exit(1)
            
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()