import subprocess

def run_scripts():
    # Run generate_v1.py
    subprocess.run(["python", "generate_v1.py"])

    # Run generate_v2.py
    subprocess.run(["python", "generate_v2.py"])

if __name__ == "__main__":
    run_scripts()
