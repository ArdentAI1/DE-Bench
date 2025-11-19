"""
This module provides a class for managing GitHub operations.
"""

import os
import re
import random
import shutil
import time
import zipfile
from typing import Any, Dict, List, Optional, Tuple, Union
import datetime
import requests
from pathlib import Path


import github
from github import Github, Repository


class GitHubManager:
    """
    A class to manage GitHub operations for testing.
    Handles repository setup, branch management, PR operations, and cleanup.
    """

    def __init__(
        self,
        access_token: str,
        repo_url: str,
        test_name: str,
        create_branch: bool = True,
        build_info: Optional[Dict[str, str]] = None,
        state_archive_path: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the GitHub manager.

        :param access_token: GitHub access token
        :param repo_url: GitHub repository URL
        """
        self.access_token = access_token
        self.repo_url = repo_url
        self.repo_name = self._parse_repo_name(repo_url)
        self.github_client = Github(access_token)
        self.repo: Repository = self.github_client.get_repo(self.repo_name)
        self.build_info = "./build-info.properties"
        self.create_branch = create_branch
        if create_branch:
            self.branch_name = self.create_test_branch(
                test_name=test_name, build_info=build_info
            )
        else:
            self.branch_name = test_name
        if state_archive_path:
            self.unpack_and_push_state_file(state_archive_path)

    def create_test_branch(
        self, test_name: str, build_info: Optional[Dict[str, str]]
    ) -> str:
        """
        Create a new branch for the test.

        :param str test_name: Name of the test
        :param Optional[Dict[str, str]] build_info: Build info dictionary to update
        :return: Name of the new branch
        :rtype: str
        """
        try:
            commit_sha = self.repo.get_commits()[0].sha
            self.repo.create_git_ref(ref=f"refs/heads/{test_name}", sha=commit_sha)
            self.branch_name = test_name
            print(f"✅ Created branch: {self.branch_name}")
        except Exception as e:
            if getattr(e, "status", None) == 422:
                print(f"Branch '{test_name}' already exists, skipping creation.")
                return getattr(self, "branch_name", test_name)
            raise Exception(f"❌ Error creating branch: {e}")
        finally:
            if build_info:
                self._update_build_info(build_info, test_name)
        return self.branch_name

    def unpack_and_push_state_file(self, state_archive_path: Union[str, Path]) -> None:
        """
        Unpack the state file and push it to the new branch

        :param Union[str, Path] state_archive_path: Path to the state archive file
        :rtype: None
        """
        if isinstance(state_archive_path, str):
            state_archive_path = Path(state_archive_path)
        if not state_archive_path.exists():
            print(f"State file {state_archive_path.absolute()} does not exist")
            return
        if not state_archive_path.is_file():
            print(f"State file {state_archive_path.absolute()} is not a file")
            return
        if state_archive_path.suffix != ".zip":
            print(f"State file {state_archive_path.absolute()} is not a zip file, it is a {state_archive_path.suffix} file!")
            return
        # make a directory for the state file, using the branch name
        state_file_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Airflow", "tmp_github_states", self.branch_name, "state_file")
        os.makedirs(state_file_dir, exist_ok=True)
        # unpack the zip and push it to the new branch
        with zipfile.ZipFile(state_archive_path.absolute(), "r") as zip_ref:
            zip_ref.extractall(state_file_dir)
        # push the extracted content of the state file to the new branch
        for root, dirs, files in os.walk(state_file_dir):
            # Skip .git and .github directories (GitHub manages these internally)
            dirs[:] = [d for d in dirs if d not in ['.git', '.github']]

            for file in files:
                local_file_path = os.path.join(root, file)
                # Compute the path relative to state_file_dir for GitHub
                rel_path = os.path.relpath(local_file_path, state_file_dir)
                # Use forward slashes for GitHub paths (even on Windows)
                github_path = rel_path.replace(os.sep, '/')

                # Skip any files that are in .git or .github directories (in case they weren't filtered by dirs)
                if github_path.startswith('.git/') or github_path.startswith('.github/'):
                    print(f"Skipping {github_path} (GitHub-managed directory)")
                    continue

                file_content = open(local_file_path, "rb").read()

                # Check if file already exists on the branch
                try:
                    existing_file = self.repo.get_contents(github_path, ref=self.branch_name)
                    # File exists, update it
                    self.repo.update_file(
                        path=github_path,
                        message=f"Update {file} in {self.branch_name} branch",
                        content=file_content,
                        sha=existing_file.sha,
                        branch=self.branch_name,
                    )
                    print(f"Updated existing file: {github_path}")
                except github.GithubException as e:
                    if e.status == 404:
                        # File doesn't exist, create it
                        self.repo.create_file(
                            path=github_path,
                            message=f"Add {file} to {self.branch_name} branch",
                            content=file_content,
                            branch=self.branch_name,
                        )
                        print(f"Created new file: {github_path}")
                    else:
                        raise
        # delete the state file directory
        shutil.rmtree(state_file_dir)
        print(f"✅ State file unpacked {state_archive_path.name} and pushed to {self.branch_name} branch")

    def add_merge_step_to_user_input(self, user_input: str, branch_name: Optional[str] = None, pr_name: Optional[str] = None) -> str:
        """
        Add detailed branching and PR instructions to user input.

        Adds explicit instructions to ensure the agent:
        1. Creates feature branch FROM the test base branch (not main)
        2. Creates PR targeting the test base branch (not main)

        :param str user_input: User input string to modify
        :return: Modified user input string with explicit branching instructions
        :param str branch_name: Name of the branch to create the feature branch from, defaults to feature/{branch_name}
        :param str pr_name: Name of the pull request to create, defaults to PR_NAME
        :rtype: str
        """
        numbers = [int(n) for n in re.findall(r"\d+", user_input)]
        last_number = max(numbers) if numbers else 0

        if not branch_name:
            branch_name = f"feature/{self.branch_name}"

        if not pr_name:
            pr_name = "PR_NAME"

        # Add explicit instructions for the branching workflow
        # These instructions ensure the agent works on the test base branch, not main
        user_input += f"\n{last_number + 1}. IMPORTANT: First checkout the '{self.branch_name}' branch (this is your base branch for this test, not main)."
        user_input += f"\n{last_number + 2}. IMPORTANT: Create your feature branch FROM the '{self.branch_name}' branch you just checked out."
        user_input += f"\n{last_number + 3}. IMPORTANT: Create your feature branch with the name '{branch_name}'."
        user_input += f"\n{last_number + 4}. IMPORTANT: When creating the pull request, set the base/target/destination branch to '{self.branch_name}' (NOT main)."
        user_input += f"\n{last_number + 5}. IMPORTANT: When creating the pull request, set the title to '{pr_name}'."

        return user_input

    @staticmethod
    def _parse_repo_name(repo_url: str) -> str:
        """
        Parse repository name from URL.

        :param str repo_url: Full GitHub repository URL
        :return: Repository name in owner/repo format
        :rtype: str
        """
        if "github.com" in repo_url:
            parts = repo_url.split("/")
            return f"{parts[-2]}/{parts[-1]}"
        return repo_url

    def _iterate_directory_and_files(
        self, folder_name: str, keep_file_names: list[str]
    ) -> None:
        for keep_file_name in keep_file_names:
            try:
                self.repo.get_contents(f"{folder_name}/{keep_file_name}")
                print(f"{folder_name}/{keep_file_name} already exists")
            except github.GithubException as e:
                if e.status == 404:
                    self.repo.create_file(
                        path=f"{folder_name}/{keep_file_name}",
                        message=f"Add {keep_file_name} to {folder_name} folder",
                        content="",
                        branch="main",
                    )
                    print(f"Created {folder_name}/{keep_file_name}")
                else:
                    raise e

    def get_multiple_file_contents_from_branch(
        self, branch_name: str, paths_to_capture: List[str]
    ) -> Dict[str, Any]:
        """
        Get multiple file/folder contents from a specific branch.

        This function captures code snapshots from the agent's work, allowing flexible
        specification of files and folders to capture. Useful for debugging agent failures
        by getting exact code state regardless of PR success/failure.

        :param str branch_name: Name of the branch to capture content from
        :param List[str] paths_to_capture: List of file paths or folder paths to capture
                                          - Paths ending with '/' are treated as folders (captures all files)
                                          - Other paths are treated as individual files
        :return: Dictionary containing captured content, metadata, and any errors
        :rtype: Dict[str, Any]

        Example:
            >>> snapshot = github_manager.get_multiple_file_contents_from_branch(
            ...     branch_name="feature/my-branch",
            ...     paths_to_capture=["dags/", "requirements.txt", "models/"]
            ... )
        """

        result = {
            "branch_name": branch_name,
            "capture_timestamp": datetime.datetime.utcnow().isoformat(),
            "captured_files": {},
            "captured_folders": {},
            "errors": [],
            "summary": {
                "total_files": 0,
                "total_size_bytes": 0,
                "folders_captured": 0,
                "files_captured": 0,
            },
        }

        print(f"📸 Capturing code snapshot from branch: {branch_name}")
        print(f"🔍 DEBUG: Starting capture with paths: {paths_to_capture}")

        for path in paths_to_capture:
            try:
                if path.endswith("/"):
                    # Treat as folder - capture all files in it
                    folder_name = path.rstrip("/")
                    print(f"📁 Capturing folder: {folder_name}")

                    try:
                        folder_contents = self.repo.get_contents(
                            folder_name, ref=branch_name
                        )
                        folder_files = {}

                        # Handle both single file and list of files
                        if not isinstance(folder_contents, list):
                            folder_contents = [folder_contents]

                        for content in folder_contents:
                            if content.type == "file":
                                file_content = content.decoded_content.decode("utf-8")
                                folder_files[content.name] = {
                                    "path": content.path,
                                    "content": file_content,
                                    "size": content.size,
                                    "sha": content.sha,
                                }
                                result["summary"]["total_files"] += 1
                                result["summary"]["total_size_bytes"] += content.size
                                print(f"  ✅ {content.name} ({content.size} bytes)")
                                print(
                                    f"🔍 DEBUG: File content preview (first 100 chars): {file_content[:100]}"
                                )

                        if folder_files:
                            result["captured_folders"][folder_name] = folder_files
                            result["summary"]["folders_captured"] += 1
                            print(
                                f"📁 Captured {len(folder_files)} files from {folder_name}"
                            )
                        else:
                            print(f"📁 No files found in {folder_name}")

                    except github.GithubException as e:
                        if e.status == 404:
                            error_msg = f"Folder not found: {folder_name}"
                            result["errors"].append(error_msg)
                            print(f"⚠️ {error_msg}")
                        else:
                            error_msg = f"Error accessing folder {folder_name}: {e}"
                            result["errors"].append(error_msg)
                            print(f"❌ {error_msg}")

                else:
                    # Treat as individual file
                    print(f"📄 Capturing file: {path}")

                    try:
                        file_content_obj = self.repo.get_contents(path, ref=branch_name)
                        if isinstance(file_content_obj, list):
                            # This shouldn't happen for individual files, but handle it
                            error_msg = f"Expected file but got directory: {path}"
                            result["errors"].append(error_msg)
                            print(f"❌ {error_msg}")
                            continue

                        file_content = file_content_obj.decoded_content.decode("utf-8")
                        result["captured_files"][path] = {
                            "path": path,
                            "content": file_content,
                            "size": file_content_obj.size,
                            "sha": file_content_obj.sha,
                        }
                        result["summary"]["total_files"] += 1
                        result["summary"]["total_size_bytes"] += file_content_obj.size
                        result["summary"]["files_captured"] += 1
                        print(f"  ✅ {path} ({file_content_obj.size} bytes)")
                        print(
                            f"🔍 DEBUG: File content preview (first 100 chars): {file_content[:100]}"
                        )

                    except github.GithubException as e:
                        if e.status == 404:
                            error_msg = f"File not found: {path}"
                            result["errors"].append(error_msg)
                            print(f"⚠️ {error_msg}")
                        else:
                            error_msg = f"Error accessing file {path}: {e}"
                            result["errors"].append(error_msg)
                            print(f"❌ {error_msg}")

            except Exception as e:
                error_msg = f"Unexpected error processing {path}: {e}"
                result["errors"].append(error_msg)
                print(f"❌ {error_msg}")

        # Final summary
        print(
            f"📸 Snapshot complete: {result['summary']['total_files']} files, "
            f"{result['summary']['total_size_bytes']} bytes, {len(result['errors'])} errors"
        )

        # Debug: Show result structure
        print(f"🔍 DEBUG: Final result structure:")
        print(f"  - captured_folders keys: {list(result['captured_folders'].keys())}")
        print(f"  - captured_files keys: {list(result['captured_files'].keys())}")
        print(f"  - errors: {result['errors']}")

        return result

    def clear_folder(
        self, folder_name: str, keep_file_names: Optional[list[str]] = None
    ) -> None:
        """
        Clear the folder and ensure .gitkeep exists and nothing else.

        :param str folder_name: Name of the folder to setup
        :param Optional[list[str]] keep_file_names: List of file names to keep in the folder, defaults to [".gitkeep"]
        :rtype: None
        """
        if keep_file_names is None:
            keep_file_names = [".gitkeep"]
        try:
            # Clear folder contents except .gitkeep
            folder_contents = self.repo.get_contents(folder_name)
            for content in folder_contents:
                if content.name not in keep_file_names:
                    self.repo.delete_file(
                        path=content.path,
                        message=f"Clear {folder_name} folder",
                        sha=content.sha,
                        branch="main",
                    )
                    print(f"Deleted file: {content.path}")
            self._iterate_directory_and_files(folder_name, keep_file_names)

        except github.GithubException as e:
            if e.status == 404:
                # Folder doesn't exist, create it with .gitkeep
                print(f"Folder '{folder_name}' doesn't exist, creating it...")
                self._iterate_directory_and_files(folder_name, keep_file_names)
            elif "sha" not in str(e):  # If error is not about folder already existing
                raise e
            else:
                print(f"⚠️ {folder_name} folder setup completed with warning: {e}")

    def verify_branch_exists(
        self, branch_name: str, test_step: Dict[str, str]
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Verify that a branch exists and update test steps.

        :param str branch_name: Name of the branch to check
        :param Dict[str, str] test_step: Test step dictionary
        :return: True if branch exists, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        print(f"Checking if branch '{branch_name}' exists...")

        # List all branches for debugging
        try:
            branches = self.repo.get_branches()
            print(
                f"Found {branches.totalCount} branches in the {self.repo_name} repository."
            )
            branch_exists = any(branch.name == branch_name for branch in branches)
            print(f"{branch_exists=}")
        except Exception as e:
            raise Exception(f"Error listing branches: {e}")

        if branch_exists:
            test_step["status"] = "passed"
            test_step["Result_Message"] = (
                f"✅ Branch '{branch_name}' was created successfully"
            )
            print(f"✓ Branch '{branch_name}' exists")
        else:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"❌ Branch '{branch_name}' was not created."
            print(f"❌ Branch '{branch_name}' was not created.")
        return branch_exists, test_step

    def find_and_merge_pr(
        self,
        pr_title: str,
        test_step: Dict[str, str],
        commit_title: Optional[str] = None,
        merge_method: str = "squash",
        build_info: Optional[Dict[str, str]] = None,
        max_retries: Optional[int] = 10,
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Find a PR by title and merge it, updating test steps.

        :param str pr_title: Title of the PR to find
        :param Dict[str, str] test_step: Test step dictionary
        :param str commit_title: Custom commit title for merge (optional)
        :param str merge_method: Merge method ('squash', 'merge', 'rebase')
        :param Dict[str, str] build_info: Build info dictionary to update (optional)
        :param int max_retries: Maximum number of retries to find the PR, defaults to 10
        :return: True if PR was found and merged, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        pulls = self.repo.get_pulls(state="open")
        target_pr = None

        print(f"Searching for PR with title: '{pr_title}'")
        print(f"Found {pulls.totalCount} open PRs:")
        for pr in pulls:
            print(f"  - PR: '{pr.title}' (branch: {pr.head.ref})")
            if pr.title == pr_title:
                target_pr = pr
                test_step["status"] = "passed"
                test_step["Result_Message"] = (
                    f"✅ PR '{pr_title}' was created successfully"
                )
                print(f"✅ Found PR: {pr_title}")
                break

        if not target_pr and max_retries > 0:
            print(
                f"❌ PR '{pr_title}' not found, retrying... ({max_retries} retries left)"
            )
            time.sleep(5)
            # Return the result of the recursive retry so the caller gets the correct status
            return self.find_and_merge_pr(
                pr_title=pr_title,
                test_step=test_step,
                commit_title=commit_title,
                merge_method=merge_method,
                build_info=build_info,
                max_retries=max_retries - 1,
            )
        elif not target_pr and max_retries <= 0:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"PR '{pr_title}' not found"
            print(f"❌ PR '{pr_title}' not found")
            return False, test_step

        # Merge the PR with retry logic for handling conflicts in parallel execution
        merge_retries = 5  # Number of merge retries for conflicts
        for merge_attempt in range(merge_retries):
            try:
                if build_info:
                    # Get the branch name from the target PR
                    branch_name = target_pr.head.ref
                    self._update_build_info(build_info, branch_name)

                print(
                    f"Attempting to merge PR '{pr_title}' (attempt {merge_attempt + 1}/{merge_retries})"
                )
                merge_result = target_pr.merge(
                    commit_title=commit_title or pr_title, merge_method=merge_method
                )

                if not merge_result.merged:
                    raise Exception(f"Merge failed: {merge_result.message}")

                print(f"✅ Successfully merged PR: {pr_title}")
                return True, test_step

            except Exception as e:
                error_message = str(e).lower()
                is_conflict = any(
                    keyword in error_message
                    for keyword in [
                        "base branch was modified",
                        "merge conflict",
                        "conflict",
                        "405",
                        "method not allowed",
                        "review and try the merge again",
                    ]
                )

                if is_conflict and merge_attempt < merge_retries - 1:
                    # Wait with exponential backoff and jitter for parallel execution conflicts
                    wait_time = (2**merge_attempt) + random.uniform(
                        1, 3
                    )  # 1-3s, 3-5s, 5-7s, etc.
                    print(
                        f"⚠️ Merge conflict detected (attempt {merge_attempt + 1}): {e}"
                    )
                    print(f"⏳ Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)

                    # Refresh the PR object to get latest state
                    try:
                        target_pr = self.repo.get_pull(target_pr.number)
                        print(f"🔄 Refreshed PR state for retry")
                    except Exception as refresh_error:
                        print(f"⚠️ Could not refresh PR state: {refresh_error}")

                    continue  # Retry the merge
                else:
                    # Non-conflict error or max retries reached
                    if is_conflict:
                        print(
                            f"❌ Failed to merge PR after {merge_retries} attempts due to persistent conflicts: {e}"
                        )
                    else:
                        print(f"❌ Failed to merge PR due to non-conflict error: {e}")
                    return False, test_step

        # Should not reach here, but just in case
        print(f"❌ Failed to merge PR after all retry attempts")
        return False, test_step

    def _update_build_info(
        self, build_info: Dict[str, str], branch_name: str
    ) -> dict[str, Any]:
        """
        Update the build_info.txt file with the provided build info dictionary.
        Verifies the change was committed before returning.

        :param Dict[str, str] build_info: Build info dictionary
        :param str branch_name: Name of the branch to update the file on
        :return: The result of the update or create operation
        :rtype: dict[str, Any]
        """
        # use github api to update the build_info.txt file
        build_info_txt = ""
        for key, value in build_info.items():
            build_info_txt += f"{key.replace(' ', '_')}={value}\n"
        build_info_txt = build_info_txt.strip()

        result = None
        try:
            contents = self.repo.get_contents(self.build_info, ref=branch_name)
            # check if the file exists
            result = self.repo.update_file(
                path=self.build_info,
                message=f"Updated {self.build_info}",
                content=build_info_txt,
                branch=branch_name,
                sha=contents.sha,
            )
            print(f"✅ Build info updated successfully for branch {branch_name}")
            print(f"Build info: {build_info_txt}")
        except Exception as e:
            if e.status == 404:
                try:
                    result = self.repo.create_file(
                        path=self.build_info,
                        message=f"Created {self.build_info}",
                        content=build_info_txt,
                        branch=branch_name,
                    )
                    print(f"✅ Build info created successfully for branch {branch_name}")
                except Exception as e:
                    raise Exception(
                        f"❌ Error creating build info for branch {branch_name}: {e}"
                    )
            else:
                raise Exception(f"Error updating build info: {e}")

        # Verify the change was committed by checking the new commit exists
        if result and "commit" in result:
            commit_sha = result["commit"].sha
            try:
                # Verify the commit exists and is accessible
                _ = self.repo.get_commit(commit_sha)
                print(f"✅ Build info commit verified: {commit_sha[:7]}")

                # Additional verification: check the file content matches what we wrote
                updated_contents = self.repo.get_contents(
                    self.build_info, ref=branch_name
                )
                if (
                    updated_contents.decoded_content.decode("utf-8").strip()
                    == build_info_txt
                ):
                    print(f"✅ Build info content verified on branch {branch_name}")
                    return result
                else:
                    print(f"⚠️ Build info content mismatch on branch {branch_name}")

            except Exception as e:
                print(f"⚠️ Could not verify build info commit: {e}")
        raise Exception(
            f"❌ Error updating/validating {self.build_info} on branch {branch_name}: {result}"
        )

    def check_and_update_gh_secrets(self, secrets: Dict[str, str]) -> None:
        """
        Checks if the GitHub secrets exists, deletes them if they do, and creates new ones with the given
            key value pairs.

        :param Dict[str, str] secrets: Dictionary of secrets to update
        :rtype: None
        """
        try:
            for secret, value in secrets.items():
                try:
                    if self.repo.get_secret(secret):
                        print(
                            f"Worker {os.getpid()}: {secret} already exists, deleting..."
                        )
                        self.repo.delete_secret(secret)
                    print(f"Worker {os.getpid()}: Creating {secret}...")
                except github.GithubException as e:
                    if e.status == 404:
                        print(
                            f"Worker {os.getpid()}: {secret} does not exist, creating..."
                        )
                    else:
                        print(
                            f"Worker {os.getpid()}: Error checking secret {secret}: {e}"
                        )
                        raise e
                self.repo.create_secret(secret, value)
                print(f"Worker {os.getpid()}: {secret} created successfully.")
        except Exception as e:
            print(
                f"Worker {os.getpid()}: Error checking and updating GitHub secrets: {e}"
            )
            raise e from e

    def delete_branch(self, branch_name: str) -> None:
        """
        Delete a branch if it exists.

        :param branch_name: Name of the branch to delete
        """
        try:
            ref = self.repo.get_git_ref(f"heads/{branch_name}")
            ref.delete()
            print(f"✅ Deleted branch: {branch_name}")
        except Exception as e:
            print(f"Branch '{branch_name}' might not exist or other error: {e}")

    def reset_repo_state(
        self, folder_name: str, keep_file_names: Optional[List[str]] = None
    ) -> None:
        """
        Reset the repository to a clean state by clearing a folder.
        This is typically called during cleanup.

        :param str folder_name: Name of the folder to clear
        :param List[str] keep_file_names: List of file names to keep in the folder, defaults to [".gitkeep"]
        :rtype: None
        """
        # TODO: may need to use a commit to reset the repo state rather than just deleting/recreating files
        if keep_file_names is None:
            keep_file_names = [".gitkeep"]

        try:
            folder_contents = self.repo.get_contents(folder_name)
            for content in folder_contents:
                if content.name not in keep_file_names:
                    self.repo.delete_file(
                        path=content.path,
                        message=f"Clear {folder_name} folder",
                        sha=content.sha,
                        branch="main",
                    )

            self._iterate_directory_and_files(folder_name, keep_file_names)
            print(f"✅ {folder_name} folder reset successfully")

        except Exception as e:
            print(f"Error resetting repository state: {e}")

    def check_if_action_is_complete(
        self,
        pr_title: str,
        wait_before_checking: Optional[int] = 60,
        max_retries: Optional[int] = 10,
        branch_name: Optional[str] = None,
        return_details: bool = False,
    ) -> Union[bool, Dict[str, Any]]:
        """
        Check if GitHub action is complete, with optional detailed status and failure info.

        :param str pr_title: Title of the PR to check the action for
        :param int wait_before_checking: Time to wait before checking if the action is complete, defaults to 60 seconds
        :param int max_retries: Maximum number of retries, defaults to 10
        :param str branch_name: Name of the branch to check
        :param bool return_details: If True, return detailed status dict; if False, return bool
        :return: Bool (success/failure) or Dict with detailed action status and failure info
        """
        print(
            f"Waiting {wait_before_checking} seconds before checking if action is complete..."
        )
        time.sleep(wait_before_checking)
        print(f"Checking GitHub action status...")
        if not branch_name:
            branch_name = self.branch_name

        latest_run = None  # Track the latest run found for potential CI details capture

        for retry in range(max_retries):
            workflow_runs = self.repo.get_workflow_runs(branch=branch_name)  # type: ignore
            if workflow_runs.totalCount > 0:
                if filtered_runs := [
                    run
                    for run in workflow_runs
                    if run.display_title.lower() == pr_title.lower()
                ]:
                    run = filtered_runs[0]
                    latest_run = run  # Keep track of the latest run

                    if run.status == "completed":
                        result = {
                            "completed": True,
                            "success": run.conclusion == "success",
                            "status": run.status,
                            "conclusion": run.conclusion,
                            "url": run.html_url,
                            "run_id": run.id,
                            "display_title": run.display_title,
                        }

                        # TESTING: Always capture CI details regardless of success/failure
                        print(
                            f"📋 Action completed - capturing CI details for testing..."
                        )
                        try:
                            ci_details = self.get_ci_failure_details(run)
                            result["ci_details"] = ci_details
                            print(
                                f"📋 Captured CI details: {len(ci_details.get('jobs', []))} jobs analyzed"
                            )
                        except Exception as e:
                            print(f"⚠️ Could not capture CI details: {e}")
                            result["ci_details_error"] = str(e)

                        print(
                            f"✅ Action completed with status: {run.status}/{run.conclusion}"
                        )

                        # Return based on return_details flag
                        if return_details:
                            return result
                        else:
                            return result.get("success", False)

                    print(f"❌ Action is not complete (status: {run.status})")
                else:
                    print(f"❌ No workflow runs found for PR title: {pr_title}")
            else:
                print(f"❌ No workflow runs found")

            print(
                f"Waiting 60 seconds before checking again...{retry + 1} of {max_retries}"
            )
            time.sleep(60)

        print(f"❌ Action did not complete after {max_retries} retries")

        # Create timeout result
        timeout_result = {
            "completed": False,
            "success": False,
            "status": "timeout",
            "conclusion": "timeout",
            "timeout_after_retries": max_retries,
        }

        # Try to capture CI details from the latest run if we found one
        if latest_run:
            try:
                ci_details = self.get_ci_failure_details(
                    latest_run, failure_override=True
                )
                timeout_result["ci_details"] = ci_details
                print(
                    f"📋 Captured CI details: {len(ci_details.get('jobs', []))} jobs analyzed"
                )
            except Exception as e:
                print(f"⚠️ Could not capture CI details: {e}")
                timeout_result["ci_details_error"] = str(e)
        else:
            print("⚠️ No workflow run found to capture CI details from")
            timeout_result["ci_details_error"] = "No workflow run found"

        # Return based on return_details flag
        if return_details:
            return timeout_result
        else:
            return timeout_result.get("success", False)

    def get_ci_failure_details(
        self, workflow_run, failure_override: Optional[bool] = False
    ) -> Dict[str, Any]:
        """
        Get detailed CI failure information from a workflow run object.

        :param workflow_run: GitHub workflow run object
        :return: Dictionary containing failure information
        """
        try:
            failure_info = {
                "workflow_run": {
                    "id": workflow_run.id,
                    "name": workflow_run.name,
                    "display_title": workflow_run.display_title,
                    "status": workflow_run.status,
                    "conclusion": workflow_run.conclusion,
                    "url": workflow_run.html_url,
                    "created_at": (
                        workflow_run.created_at.isoformat()
                        if workflow_run.created_at
                        else None
                    ),
                    "updated_at": (
                        workflow_run.updated_at.isoformat()
                        if workflow_run.updated_at
                        else None
                    ),
                    "head_branch": workflow_run.head_branch,
                    "head_sha": workflow_run.head_sha,
                },
                "jobs": [],
                "summary": f"{'✅' if workflow_run.conclusion == 'success' else '❌'} Workflow '{workflow_run.name}' {workflow_run.conclusion} (status: {workflow_run.status})",
            }

            # Get job details for workflow run
            print(f"🔧 Fetching job details for workflow run...")
            try:
                for job in workflow_run.jobs():
                    job_info = {
                        "id": job.id,
                        "name": job.name,
                        "status": job.status,
                        "conclusion": job.conclusion,
                        "url": job.html_url,
                        "started_at": (
                            job.started_at.isoformat() if job.started_at else None
                        ),
                        "completed_at": (
                            job.completed_at.isoformat() if job.completed_at else None
                        ),
                    }

                    # Add step details for all jobs (for testing - we can filter later)
                    job_info["steps"] = []
                    for step in job.steps:
                        step_info = {
                            "name": step.name,
                            "status": step.status,
                            "conclusion": step.conclusion,
                            "number": step.number,
                            "started_at": (
                                step.started_at.isoformat() if step.started_at else None
                            ),
                            "completed_at": (
                                step.completed_at.isoformat()
                                if step.completed_at
                                else None
                            ),
                        }
                        job_info["steps"].append(step_info)

                    # Capture full logs only on failure, steps always captured
                    if job.conclusion == "failure" or failure_override:
                        print(
                            f"📋 Job failed - fetching full logs for: {job.name} (ID: {job.id})"
                        )
                        try:
                            # Parse owner and repo name from self.repo_name (format: "owner/repo")
                            owner, repo_name = self.repo_name.split("/")

                            url = f"https://api.github.com/repos/{owner}/{repo_name}/actions/jobs/{job.id}/logs"

                            headers = {
                                "Authorization": f"token {self.access_token}",
                                "Accept": "application/vnd.github.v3+json",
                            }

                            response = requests.get(url, headers=headers)
                            response.raise_for_status()
                            job_logs = response.text

                            job_info["logs"] = job_logs
                            print(
                                f"✅ Fetched {len(job_logs)} characters of failure logs for job {job.name}"
                            )
                        except Exception as e:
                            print(
                                f"⚠️ Could not fetch failure logs for job {job.id}: {e}"
                            )
                            job_info["logs"] = None
                            job_info["logs_error"] = str(e)
                    else:
                        print(
                            f"📋 Job succeeded - skipping logs for: {job.name} (steps still captured)"
                        )
                        job_info["logs"] = None

                    failure_info["jobs"].append(job_info)

            except Exception as e:
                print(f"⚠️ Could not fetch job details: {e}")
                failure_info["job_fetch_error"] = str(e)

            return failure_info

        except Exception as e:
            print(f"⚠️ Error getting CI failure details: {e}")
            return {"error": "Failed to get CI details", "details": str(e)}

    def cleanup_requirements(self, requirements_path: str = "Requirements/") -> None:
        """
        Reset requirements.txt to blank.

        :param str requirements_path: Path to requirements folder
        :rtype: None
        """
        try:
            requirements_file = self.repo.get_contents(
                os.path.join(requirements_path, "requirements.txt")
            )
            # check if the file has no content before resetting it
            if requirements_file.decoded_content == b"":
                print("✓ Requirements.txt is already blank")
                return
            self.repo.update_file(
                path=requirements_file.path,
                message="Reset requirements.txt to blank",
                content="",
                sha=requirements_file.sha,
                branch="main",
            )
            print("✓ Requirements.txt reset successfully")
        except Exception as e:
            print(f"Error cleaning up requirements: {e}")

    def get_repo_info(self) -> Dict[str, str]:
        """
        Get repository information.

        :return: Dictionary with repository information
        """
        return {
            "repo_name": self.repo_name,
            "repo_url": self.repo_url,
            "default_branch": self.repo.default_branch,
        }
