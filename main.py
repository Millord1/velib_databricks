import sys
import os

def main():
    # Define main.py absolute path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # get arg from input (terminal)
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    
    from src.utils.db_factory import get_db_connector
    from src.pipelines.historical_pipeline import run_historical_pipeline
    # from src.pipelines.incremental_pipeline import run_incremental_pipeline
    
    try:
        # get right db depending on environement (local or databricks) from db factory
        db_connector = get_db_connector()
        
        if mode == "historical":
            print("Start import historical data")
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