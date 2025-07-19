"""
This class is used to start and stop the Airflow local instance using docker compose.
"""

import os
import shutil
import subprocess
import time
from typing import Optional

from git import Repo, GitCommandError, InvalidGitRepositoryError
from python_on_whales import DockerClient


class Airflow_Local:
    def __init__(self, port: Optional[int] = None):
        self.Airflow_DIR = os.path.dirname(os.path.abspath(__file__))
        self.Airflow_USERNAME = os.getenv("AIRFLOW_USERNAME")
        self.Airflow_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
        self.Airflow_HOST = os.getenv("AIRFLOW_HOST", "localhost")
        self.Airflow_PORT = port or "8888"
        # make sure the host doesn't have http:// or https://
        url_prefix = "" if self.Airflow_HOST.startswith("http") else "http://"
        self.Airflow_BASE_URL = f"{url_prefix}{self.Airflow_HOST}:{self.Airflow_PORT}"

    def Start_Airflow(self, public_expose=False):
        # Set the absolute path for AIRFLOW_PROJ_DIR
        os.environ["AIRFLOW_PROJ_DIR"] = self.Airflow_DIR

        # Boot up the docker instance for compose
        docker = DockerClient(
            compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
        )

        # Docker compose up to get it to start
        docker.compose.up(detach=True)

        # Wait for the airflow webserver to start
        print("Waiting for Airflow to start... (Sleeping for 30 seconds)")
        time.sleep(30)

        if public_expose:
            # TODO: Implement this for external connections. For now we ignore
            print("TODO LATER")

    def Stop_Airflow(self):

        # load the docker compose as docker
        docker = DockerClient(
            compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
        )

        # docker down
        docker.compose.down(volumes=True)

    def Get_Airflow_Dags_From_Github(self):
        """
        Clone the GitHub repository and copy DAG files to the Airflow dags directory.
        Uses environment variables from .env file.
        """

        github_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
        repo_url = os.getenv("AIRFLOW_REPO")
        dag_path = os.getenv("AIRFLOW_DAG_PATH", "dags/")
        
        # Debug: Print current working directory and Airflow directory
        print(f"Current working directory: {os.getcwd()}")
        print(f"Airflow directory: {self.Airflow_DIR}")
        print(f"Destination DAG path: {os.path.join(self.Airflow_DIR, 'dags')}")

        # Validate environment variables
        if not github_token:
            raise ValueError(
                "The AIRFLOW_GITHUB_TOKEN environment variable is not set."
            )
        if not repo_url:
            raise ValueError("The AIRFLOW_REPO environment variable is not set.")

        # Prepare the repository URL with authentication
        if repo_url.startswith("https://"):
            repo_url = repo_url.replace("https://", f"https://{github_token}@")
        else:
            raise ValueError("The AIRFLOW_REPO URL must start with 'https://'.")

        # Define local paths
        git_repo_path = os.path.join(self.Airflow_DIR, "GitRepo")
        destination_dag_path = os.path.join(self.Airflow_DIR, "dags")

        # Ensure the destination DAG directory exists with proper permissions
        print(f"Creating/checking destination DAG directory: {destination_dag_path}")
        if not os.path.exists(destination_dag_path):
            print(f"Creating directory: {destination_dag_path}")
            os.makedirs(destination_dag_path, mode=0o755)
        else:
            print(f"Directory already exists: {destination_dag_path}")
            # Ensure existing directory has proper permissions
            try:
                os.chmod(destination_dag_path, 0o755)
                print(f"Set directory permissions to 0o755")
            except (PermissionError, OSError) as e:
                print(f"Could not change directory permissions: {e}")
                pass  # Continue even if we can't change permissions
        
        # Verify directory is writable
        try:
            test_file = os.path.join(destination_dag_path, ".test_write")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            print(f"Directory {destination_dag_path} is writable")
        except Exception as e:
            print(f"Warning: Directory {destination_dag_path} is not writable: {e}")
            # Try to fix permissions
            try:
                os.system(f"chmod -R 755 '{destination_dag_path}'")
                print(f"Attempted to fix permissions on {destination_dag_path}")
            except Exception as chmod_e:
                print(f"Could not fix permissions: {chmod_e}")

        try:
            # Check if the directory exists and is a valid git repository
            is_valid_repo = False
            if os.path.exists(git_repo_path):
                try:
                    # Check if directory has anything other than .gitkeep
                    contents = os.listdir(git_repo_path)
                    if len(contents) == 1 and contents[0] == ".gitkeep":
                        pass
                    else:
                        repo = Repo(git_repo_path)
                        is_valid_repo = True

                        # Check remote URL
                        origin = repo.remotes.origin

                        # Force update remote URL with token
                        if repo_url != origin.url:
                            origin.set_url(repo_url)

                        repo.git.reset("--hard", "origin/main")

                        pull_info = origin.pull()
                        for info in pull_info:
                            pass

                        source_dag_path = os.path.join(git_repo_path, dag_path)
                except (InvalidGitRepositoryError, GitCommandError) as e:
                    # Clean contents instead of removing directory
                    for item in os.listdir(git_repo_path):
                        if item != ".gitkeep":  # Preserve .gitkeep
                            item_path = os.path.join(git_repo_path, item)
                            if os.path.isfile(item_path):
                                os.unlink(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                    is_valid_repo = False

            # Clone the repository if it doesn't exist or wasn't valid
            if not is_valid_repo:
                # Create directory if it doesn't exist
                os.makedirs(git_repo_path, exist_ok=True)

                # Clean everything except .gitkeep before cloning
                for item in os.listdir(git_repo_path):
                    if item != ".gitkeep":  # Preserve .gitkeep
                        item_path = os.path.join(git_repo_path, item)
                        if os.path.isfile(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)

                # Temporarily move .gitkeep if it exists
                gitkeep_exists = os.path.exists(os.path.join(git_repo_path, ".gitkeep"))
                if gitkeep_exists:
                    os.rename(
                        os.path.join(git_repo_path, ".gitkeep"),
                        os.path.join(self.Airflow_DIR, ".gitkeep_temp"),
                    )

                try:
                    # Clone into the now-empty directory
                    Repo.clone_from(repo_url, git_repo_path)
                finally:
                    # Restore .gitkeep if it existed
                    if gitkeep_exists:
                        os.rename(
                            os.path.join(self.Airflow_DIR, ".gitkeep_temp"),
                            os.path.join(git_repo_path, ".gitkeep"),
                        )

            # After cloning the repository and before copying DAGs
            self.Update_Airflow_Requirements()

        except GitCommandError as e:
            raise RuntimeError(f"Git operation failed: {e}")

        # Define the source path for DAG files
        source_dag_path = os.path.join(git_repo_path, dag_path)

        # Check if the source DAG path exists
        if not os.path.exists(source_dag_path):
            raise FileNotFoundError(
                f"The specified DAG path {source_dag_path} does not exist in the repository."
            )

        # Copy DAG files to the Airflow DAGs directory
        for item in os.listdir(source_dag_path):
            s = os.path.join(source_dag_path, item)
            d = os.path.join(destination_dag_path, item)
            
            # Check if source file/directory exists and is accessible
            if not os.path.exists(s):
                print(f"Warning: Source path does not exist: {s}")
                continue
                
            if not os.access(s, os.R_OK):
                print(f"Warning: Source path is not readable: {s}")
                continue
            if os.path.isdir(s):
                if os.path.exists(d):
                    shutil.rmtree(d)
                shutil.copytree(s, d)
            else:
                # Ensure destination directory has proper permissions
                try:
                    os.chmod(destination_dag_path, 0o755)
                except (PermissionError, OSError):
                    pass  # Continue even if we can't change directory permissions
                
                # Handle file copying with comprehensive error handling
                try:
                    # Debug: Print paths for troubleshooting
                    print(f"Copying from: {s}")
                    print(f"Copying to: {d}")
                    
                    # If destination file exists, try to remove it first
                    if os.path.exists(d):
                        print(f"Destination file exists, attempting to remove: {d}")
                        # Try multiple approaches to remove the file
                        removed = False
                        
                        # Approach 1: Normal remove
                        try:
                            os.chmod(d, 0o666)  # Make writable
                            os.remove(d)
                            removed = True
                            print(f"Successfully removed existing file: {d}")
                        except (PermissionError, OSError) as e:
                            print(f"Normal remove failed: {e}")
                        
                        # Approach 2: Force remove with system command
                        if not removed:
                            try:
                                result = os.system(f"rm -f '{d}'")
                                if result == 0:
                                    removed = True
                                    print(f"Successfully removed file using rm command: {d}")
                                else:
                                    print(f"rm command failed with exit code: {result}")
                            except Exception as rm_e:
                                print(f"rm command failed: {rm_e}")
                        
                        # Approach 3: Try with different permissions
                        if not removed:
                            try:
                                os.system(f"chmod 777 '{d}'")
                                os.remove(d)
                                removed = True
                                print(f"Successfully removed file after chmod: {d}")
                            except Exception as chmod_e:
                                print(f"chmod + remove failed: {chmod_e}")
                        
                        if not removed:
                            print(f"Warning: Could not remove existing file {d}, skipping...")
                            continue
                    
                    # Ensure the destination directory exists and is writable
                    dest_dir = os.path.dirname(d)
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir, mode=0o755)
                    else:
                        try:
                            os.chmod(dest_dir, 0o755)
                        except (PermissionError, OSError):
                            pass
                    
                    # Try to copy the file
                    print(f"Attempting to copy file using shutil.copy2...")
                    try:
                        shutil.copy2(s, d)
                        print(f"Successfully copied file using shutil.copy2")
                    except Exception as copy_error:
                        print(f"shutil.copy2 failed: {copy_error}")
                        # Try manual copy as fallback
                        try:
                            with open(s, 'rb') as fsrc:
                                with open(d, 'wb') as fdst:
                                    fdst.write(fsrc.read())
                            print(f"Successfully copied file using manual method")
                        except Exception as manual_error:
                            print(f"Manual copy also failed: {manual_error}")
                            raise
                    
                    # Ensure the copied file has proper permissions
                    try:
                        os.chmod(d, 0o644)  # Readable by all, writable by owner
                        print(f"Set file permissions to 0o644")
                    except (PermissionError, OSError) as perm_error:
                        print(f"Could not set file permissions: {perm_error}")
                        pass  # Continue even if we can't set permissions
                        
                except Exception as e:
                    print(f"Unexpected error copying {s} to {d}: {e}")
                    continue

    def Cleanup_Airflow_Directories(self):
        """
        Clean up all Airflow directories while preserving .gitkeep files.
        Also resets the database to clear all DAG history and metadata.
        Includes: dags, GitRepo, config, logs, plugins directories and requirements cache.
        """
        try:
            # Reset the Airflow database first
            # docker = DockerClient(
            #     compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
            # )
            # try:
            #     docker.compose.execute(
            #         "airflow-webserver", ["airflow", "db", "reset", "-y"]
            #     )
            # except Exception as e:
            #     print(
            #         f"Note: Could not reset database (this is okay if Airflow is not running): {e}"
            #     )

            # Define all directories to clean
            directories = {
                "dags": os.path.join(self.Airflow_DIR, "dags"),
                "git_repo": os.path.join(self.Airflow_DIR, "GitRepo"),
                "config": os.path.join(self.Airflow_DIR, "config"),
                "logs": os.path.join(self.Airflow_DIR, "logs"),
                "plugins": os.path.join(self.Airflow_DIR, "plugins"),
            }

            # Clean requirements cache
            cache_path = os.path.join(self.Airflow_DIR, ".requirements_cache")
            if os.path.exists(cache_path):
                print("Removing requirements cache...")
                os.remove(cache_path)

            # Reset the permanent requirements.txt file to be empty
            permanent_requirements_path = os.path.join(self.Airflow_DIR, "Requirements", "requirements.txt")
            if os.path.exists(permanent_requirements_path):
                print("Resetting requirements.txt file...")
                with open(permanent_requirements_path, 'w') as f:
                    pass  # Make the file empty

            # Clean each directory
            for dir_name, dir_path in directories.items():
                if os.path.exists(dir_path):
                    try:
                        for item in os.listdir(dir_path):
                            if item != ".gitkeep":  # Preserve .gitkeep
                                item_path = os.path.join(dir_path, item)
                                if os.path.isfile(item_path):
                                    os.unlink(item_path)
                                elif os.path.isdir(item_path):
                                    shutil.rmtree(item_path)

                        # Ensure .gitkeep exists
                        gitkeep_path = os.path.join(dir_path, ".gitkeep")
                        if not os.path.exists(gitkeep_path):
                            with open(gitkeep_path, "w") as f:
                                pass

                    except Exception as e:
                        print(f"Error while cleaning {dir_name} directory: {e}")
                else:
                    print(f"{dir_name} directory does not exist: {dir_path}")

            return True
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False
        
    def Update_Airflow_Requirements(self):
        """
        Check if requirements.txt has changed and rebuild the Airflow
        containers only if necessary.
        
        Returns:
            bool: True if containers were rebuilt, False otherwise
        """
        requirements_path = os.path.join(self.Airflow_DIR, "GitRepo", "Requirements", "requirements.txt")
        permanent_requirements_path = os.path.join(self.Airflow_DIR, "Requirements", "requirements.txt")
        cache_path = os.path.join(self.Airflow_DIR, ".requirements_cache")
        
        # If requirements file doesn't exist, nothing to do
        if not os.path.exists(requirements_path):
            print("No requirements.txt found, skipping requirements update")
            return False
        
        # Check if requirements file is empty
        with open(requirements_path, 'r') as f:
            requirements_content = f.read().strip()
            if not requirements_content:
                print("Requirements file is empty, skipping update")
                return False
        
        # Check if requirements have changed from previous run
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                cached_content = f.read().strip()
                if cached_content == requirements_content:
                    print("Requirements unchanged, skipping rebuild")
                    return False
                
        
        print("Requirements have changed, rebuilding Airflow containers...")

        #now lets copy to the permanent requirements file
        with open(requirements_path, 'r') as src:
            content = src.read()
            with open(permanent_requirements_path, 'w') as dst:
                dst.write(content)

        
        # Save current requirements to cache
        with open(cache_path, 'w') as f:
            f.write(requirements_content)
        
        # Initialize Docker client
        docker = DockerClient(
            compose_files=[os.path.join(self.Airflow_DIR, "docker-compose.yml")]
        )

        print("Docker client context:")
        print(f"Docker client context: {docker.context}")
        
        try:
            # Stop existing containers
            print("Stopping existing containers...")
            docker.compose.down(volumes=True)

            time.sleep(10)  # Increased wait time to ensure services are ready

            
            # Rebuild and start containers
            print("Building containers with no cache...")
            
            subprocess.run(
            ["docker", "compose", "-f", os.path.join(self.Airflow_DIR, "docker-compose.yml"), "build", "--no-cache"],
            check=True
        )  # Force clean build every time

            print("Starting containers...")
            docker.compose.up(detach=True)
            
            # Wait for the services to be ready
            print("Waiting for services to be ready...")
            time.sleep(30)  # Increased wait time to ensure services are ready
            
            print("Airflow containers rebuilt and restarted with new requirements")
            return True
            
        except Exception as e:
            print(f"Error during container rebuild: {e}")
            # Try to ensure containers are running even if there was an error
            try:
                docker.compose.up(detach=True)
            except:
                pass
            raise
