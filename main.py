import sys
import os
import traceback
import argparse

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
    try:
        root_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        root_path = os.getcwd()
    
    # Gestion robuste des arguments (ignore le -f système de Databricks)
    parser = argparse.ArgumentParser(description="Pipeline Vélib")
    parser.add_argument("--mode", type=str, default="historical", choices=["historical", "incremental"])
    args, unknown = parser.parse_known_args()
    mode = args.mode
    
    from src.utils.db_factory import get_db_connector
    from src.pipelines.historical_pipeline import run_historical_pipeline
    
    try:
        db_connector = get_db_connector()
        
        if mode == "historical":
            print("Start importing historical data")
            run_historical_pipeline(db_connector)
            
        elif mode == "incremental":
            pass
            
    except Exception as e:
        raise e


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)