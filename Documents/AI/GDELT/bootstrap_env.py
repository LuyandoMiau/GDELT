""" THIS SCRIPT SETS UP A CONDA ENVIRONMENT FOR GDELT WORKFLOWS.
It checks if the environment exists, creates it if not, installs required packages,
and provides instructions to activate it."""

import subprocess
import sys
import json

# Name of the environment
ENV_NAME = "GDELT"

# Full list of packages you want installed
REQUIRED_PACKAGES = [
    # Core third‑party packages
    "pandas",
    "numpy",
    "requests",
    # Sometime used packages for GDELT workflows
    "pyarrow",
    "tqdm",
    "beautifulsoup4",
    "lxml",
    "openpyxl"
]

""" FUNCTION 1: CHECK IF CONDA ENVIRONMENT EXISTS """

def conda_env_exists(env_name: str) -> bool:
    """Check if a conda environment already exists."""
    try:
        result = subprocess.run(
            ["conda", "env", "list", "--json"],
            capture_output=True,
            text=True,
            check=True
        )
        envs = json.loads(result.stdout)["envs"]
        return any(env_name in path for path in envs)
    except Exception as e:
        print(f"Error checking conda environments: {e}")
        sys.exit(1)

""" FUNCTION 2: CREATE CONDA ENVIRONMENT IF MISSING """

def create_conda_env(env_name: str):
    """Create the conda environment if it does not exist."""
    print(f"Creating environment '{env_name}'...")
    subprocess.run(
        ["conda", "create", "-y", "-n", env_name, "python=3.11"],
        check=True
    )
    print(f"Environment '{env_name}' created.")

""" FUNCTION 3: INSTALL PACKAGES INTO ENVIRONMENT """

def install_packages(env_name: str, packages: list):
    """Install required packages into the environment."""
    print(f"Installing packages into '{env_name}' using pip...")
    # Using pip instead of conda for faster, more reliable installation
    subprocess.run(
        ["conda", "run", "-n", env_name, "pip", "install", "--no-cache-dir"] + packages,
        check=True
    )
    print("Package installation complete.")

"""+++++ MAIN FUNCTION TO RUN THE SETUP PER STEPS +++++"""

def main():
    print(f"Checking if environment '{ENV_NAME}' exists...")

    # Step 1 — Create environment if missing
    if not conda_env_exists(ENV_NAME):
        create_conda_env(ENV_NAME)
    else:
        print(f"Environment '{ENV_NAME}' already exists. Skipping creation.")

    # Step 2 — Install packages
    install_packages(ENV_NAME, REQUIRED_PACKAGES)

    # Step 3 — Tell user how to activate
    print("\nSetup complete.")
    print(f"To activate the environment, run:")
    print(f"\n    conda activate {ENV_NAME}\n")


if __name__ == "__main__":
    main()