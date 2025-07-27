"""
This module provides a class for managing GitHub operations.
"""

import os
from typing import Dict, List, Optional, Tuple

import github
from github import Github


class GitHubManager:
    """
    A class to manage GitHub operations for testing.
    Handles repository setup, branch management, PR operations, and cleanup.
    """
    
    def __init__(self, access_token: str, repo_url: str):
        """
        Initialize the GitHub manager.
        
        :param access_token: GitHub access token
        :param repo_url: GitHub repository URL
        """
        self.access_token = access_token
        self.repo_url = repo_url
        self.repo_name = self._parse_repo_name(repo_url)
        self.github_client = Github(access_token)
        self.repo = self.github_client.get_repo(self.repo_name)
        self.created_branches = []
        self.created_files = []

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

    def _iterate_directory_and_files(self, folder_name: str, keep_file_names: list[str]) -> None:
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
    
    def clear_folder(self, folder_name: str, keep_file_names: Optional[list[str]] = None) -> None:
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
            if "sha" not in str(e):  # If error is not about folder already existing
                raise e
            print(f"{folder_name} folder setup completed with warning: {e}")
    
    def verify_branch_exists(self, branch_name: str, test_step: Dict[str, str]) -> Tuple[bool, Dict[str, str]]:
        """
        Verify that a branch exists and update test steps.
        
        :param str branch_name: Name of the branch to check
        :param Dict[str, str] test_step: Test step dictionary
        :return: True if branch exists, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        try:
            _ = self.repo.get_branch(branch_name)
            test_step["status"] = "passed"
            test_step["Result_Message"] = f"Branch '{branch_name}' was created successfully"
            print(f"✓ Branch '{branch_name}' exists")
            return True, test_step
        except Exception as e:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"Branch '{branch_name}' was not created: {str(e)}"
            print(f"✗ Branch '{branch_name}' was not created: {str(e)}")
            return False, test_step
    
    def find_and_merge_pr(self, pr_title: str, test_step: Dict[str, str], commit_title: Optional[str] = None, merge_method: str = "squash") -> Tuple[bool, Dict[str, str]]:
        """
        Find a PR by title and merge it, updating test steps.
        
        :param str pr_title: Title of the PR to find
        :param Dict[str, str] test_step: Test step dictionary
        :param str commit_title: Custom commit title for merge (optional)
        :param str merge_method: Merge method ('squash', 'merge', 'rebase')
        :return: True if PR was found and merged, False otherwise, and the updated test step
        :rtype: Tuple[bool, Dict[str, str]]
        """
        pulls = self.repo.get_pulls(state="open")
        target_pr = None
        
        for pr in pulls:
            if pr.title == pr_title:
                target_pr = pr
                test_step["status"] = "passed"
                test_step["Result_Message"] = f"PR '{pr_title}' was created successfully"
                print(f"✓ Found PR: {pr_title}")
                break
        
        if not target_pr:
            test_step["status"] = "failed"
            test_step["Result_Message"] = f"PR '{pr_title}' not found"
            print(f"✗ PR '{pr_title}' not found")
            return False, test_step
        
        # Merge the PR
        try:
            merge_result = target_pr.merge(
                commit_title=commit_title or pr_title,
                merge_method=merge_method
            )
            
            if not merge_result.merged:
                raise Exception(f"Failed to merge PR: {merge_result.message}")
            
            print(f"✓ Successfully merged PR: {pr_title}")
            return True, test_step
            
        except Exception as e:
            print(f"✗ Failed to merge PR: {e}")
            return False, test_step
    
    def delete_branch(self, branch_name: str) -> None:
        """
        Delete a branch if it exists.
        
        :param branch_name: Name of the branch to delete
        """
        try:
            ref = self.repo.get_git_ref(f"heads/{branch_name}")
            ref.delete()
            print(f"✓ Deleted branch: {branch_name}")
        except Exception as e:
            print(f"Branch '{branch_name}' might not exist or other error: {e}")
    
    def reset_repo_state(self, folder_name: str, keep_file_names: Optional[List[str]] = None) -> None:
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
            print(f"✓ {folder_name} folder reset successfully")
            
        except Exception as e:
            print(f"Error resetting repository state: {e}")
    
    def cleanup_requirements(self, requirements_path: str = "Requirements/") -> None:
        """
        Reset requirements.txt to blank.
        
        :param str requirements_path: Path to requirements folder
        :rtype: None
        """
        try:
            requirements_file = self.repo.get_contents(os.path.join(requirements_path, "requirements.txt"))
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
            "default_branch": self.repo.default_branch
        }
