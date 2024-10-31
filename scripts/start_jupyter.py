from dotenv import load_dotenv
import subprocess

# Load environment variables from .env file
load_dotenv()

# Start Jupyter Notebook
subprocess.run(["jupyter", "notebook", "--no-browser", "--ip=0.0.0.0"])
