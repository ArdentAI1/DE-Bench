#!/usr/bin/env python3
"""
Script to clean up GitHub PRs and branches that match test naming patterns.

This script identifies and deletes PRs where the source or destination branch
ends with patterns like 'XXXXXXXXXX-1758651615_ed254e3b' (timestamp_hash format)
or starts with 'test_airflow_'. Uses threading for parallel processing with
retry logic to handle rate limiting.
"""

import os
import re
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from threading import Lock
from typing import List, Tuple, Dict, Any, Optional, Callable

import github
from github import Github, Repository
from dotenv import load_dotenv


def load_environment() -> Tuple[str, str]:
    """
    Load environment variables from .env file.
    
    Returns:
        Tuple of (github_token, repo_url)
        
    Raises:
        ValueError: If required environment variables are not set
    """
    # Load .env file from the project root
    load_dotenv()
    
    github_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
    repo_url = os.getenv("AIRFLOW_REPO")
    
    if not github_token:
        raise ValueError("AIRFLOW_GITHUB_TOKEN environment variable is not set")
    if not repo_url:
        raise ValueError("AIRFLOW_REPO environment variable is not set")
    
    return github_token, repo_url


def parse_repo_name(repo_url: str) -> str:
    """
    Parse repository name from URL.
    
    Args:
        repo_url: Full GitHub repository URL
        
    Returns:
        Repository name in owner/repo format
    """
    if "github.com" in repo_url:
        parts = repo_url.split("/")
        return f"{parts[-2]}/{parts[-1]}"
    return repo_url


def matches_test_branch_pattern(branch_name: str) -> bool:
    """
    Check if a branch name matches the test branch naming patterns.
    
    Patterns supported:
    1. Ends with '-{timestamp}_{hash}' where timestamp is digits and hash is alphanumeric
    2. Starts with 'test_airflow_'
    
    Examples:
    - 'some_test_branch-1758652260_60884125' -> True
    - 'feature-1758651615_ed254e3b' -> True
    - 'test_airflow_simple_dag' -> True
    - 'test_airflow_workflow_123' -> True
    - 'main' -> False
    - 'develop' -> False
    
    Args:
        branch_name: Name of the branch to check
        
    Returns:
        True if branch matches any test pattern, False otherwise
    """
    # Pattern 1: anything followed by dash, digits, underscore, alphanumeric chars at end
    timestamp_hash_pattern = r'.*-\d+_[a-zA-Z0-9]+$'
    
    # Pattern 2: starts with 'test_airflow_'
    airflow_test_pattern = r'^test_airflow_.*'

    # Pattern 3: starts with 'feature/test_'
    feature_test_pattern = r'^feature/test_.*'

    possible_patterns = [timestamp_hash_pattern, airflow_test_pattern, feature_test_pattern]
    for pattern in possible_patterns:
        if bool(re.match(pattern, branch_name)):
            return True
    return False


def get_prs_with_test_branches(repo: Repository) -> List[Dict[str, Any]]:
    """
    Get all PRs that have source or destination branches matching test patterns.
    
    Args:
        repo: GitHub repository object
        
    Returns:
        List of PR information dictionaries with test branch matches
    """
    print("🔍 Fetching all open PRs from repository...")
    
    prs_to_clean = []
    all_prs = repo.get_pulls(state="open")
    
    print(f"📋 Found {all_prs.totalCount} open PRs, checking for test branch patterns...")
    
    for pr in all_prs:
        source_branch = pr.head.ref
        dest_branch = pr.base.ref
        
        source_matches = matches_test_branch_pattern(source_branch)
        dest_matches = matches_test_branch_pattern(dest_branch)
        
        if source_matches or dest_matches:
            pr_info = {
                "number": pr.number,
                "title": pr.title,
                "source_branch": source_branch,
                "dest_branch": dest_branch,
                "source_matches": source_matches,
                "dest_matches": dest_matches,
                "url": pr.html_url,
                "pr_object": pr
            }
            prs_to_clean.append(pr_info)
            print(f"  ✅ Found PR #{pr.number}: '{pr.title}'")
            print(f"     Source: {source_branch} {'✓' if source_matches else '✗'}")
            print(f"     Dest: {dest_branch} {'✓' if dest_matches else '✗'}")
    
    return prs_to_clean


def get_all_test_branches(repo: Repository) -> List[str]:
    """
    Get all branches that match the test branch pattern.
    
    Args:
        repo: GitHub repository object
        
    Returns:
        List of branch names that match test patterns
    """
    print("🌲 Fetching all branches to identify test branches...")
    
    test_branches = []
    all_branches = repo.get_branches()
    
    for branch in all_branches:
        if matches_test_branch_pattern(branch.name):
            test_branches.append(branch.name)
            print(f"  ✅ Found test branch: {branch.name}")
    
    return test_branches


def display_cleanup_summary(prs_to_clean: List[Dict[str, Any]], test_branches: List[str]) -> None:
    """
    Display a summary of what will be cleaned up.
    
    Args:
        prs_to_clean: List of PR information dictionaries
        test_branches: List of test branch names
    """
    print("\n" + "="*80)
    print("🧹 CLEANUP SUMMARY")
    print("="*80)
    
    if prs_to_clean:
        print(f"\n📝 Found {len(prs_to_clean)} PRs to clean up:")
        for pr_info in prs_to_clean:
            print(f"\n  PR #{pr_info['number']}: {pr_info['title']}")
            print(f"    URL: {pr_info['url']}")
            print(f"    Source branch: {pr_info['source_branch']} {'(matches pattern)' if pr_info['source_matches'] else ''}")
            print(f"    Destination branch: {pr_info['dest_branch']} {'(matches pattern)' if pr_info['dest_matches'] else ''}")
    else:
        print("\n✅ No PRs found that match the test branch patterns")
    
    if test_branches:
        print(f"\n🌲 Found {len(test_branches)} test branches:")
        for branch in test_branches:
            print(f"    - {branch}")
    else:
        print("\n✅ No additional test branches found")
    
    if not prs_to_clean and not test_branches:
        print("\n🎉 Repository is clean! No test PRs or branches to remove.")


def get_user_confirmation() -> bool:
    """
    Get user confirmation before proceeding with cleanup.
    
    Returns:
        True if user confirms, False otherwise
    """
    print("\n" + "="*80)
    print("⚠️  CONFIRMATION REQUIRED")
    print("="*80)
    
    while True:
        response = input("\nDo you want to proceed with the cleanup? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Please enter 'yes' or 'no'")


def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator that implements retry logic with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except github.GithubException as e:
                    last_exception = e
                    
                    # Handle rate limiting specifically
                    if e.status == 403 and "rate limit" in str(e).lower():
                        if attempt < max_retries:
                            # Get reset time from headers if available
                            reset_time = getattr(e, 'headers', {}).get('X-RateLimit-Reset')
                            if reset_time:
                                wait_time = min(int(reset_time) - int(time.time()) + 5, max_delay)
                            else:
                                wait_time = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                            
                            print(f"    ⏳ Rate limit hit, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                            time.sleep(wait_time)
                            continue
                    
                    # Handle other GitHub exceptions with exponential backoff
                    elif e.status in [500, 502, 503, 504] and attempt < max_retries:
                        wait_time = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                        print(f"    ⏳ Server error {e.status}, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                    
                    # Don't retry for other GitHub exceptions
                    else:
                        break
                        
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                        print(f"    ⏳ Unexpected error, waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        break
            
            # If we get here, all retries failed
            raise last_exception
        
        return wrapper
    return decorator


@retry_with_backoff(max_retries=3)
def close_pr_with_retry(pr) -> Dict[str, Any]:
    """
    Close a PR with retry logic.
    
    Args:
        pr: GitHub PR object
        
    Returns:
        Dictionary with operation result
    """
    pr.edit(state='closed')
    return {
        "success": True,
        "pr_number": pr.number,
        "pr_title": pr.title,
        "message": f"PR #{pr.number} closed successfully"
    }


@retry_with_backoff(max_retries=3)
def delete_branch_with_retry(repo: Repository, branch_name: str) -> Dict[str, Any]:
    """
    Delete a branch with retry logic.
    
    Args:
        repo: GitHub repository object
        branch_name: Name of the branch to delete
        
    Returns:
        Dictionary with operation result
    """
    ref = repo.get_git_ref(f"heads/{branch_name}")
    ref.delete()
    return {
        "success": True,
        "branch_name": branch_name,
        "message": f"Branch {branch_name} deleted successfully"
    }


def cleanup_prs_and_branches(repo: Repository, prs_to_clean: List[Dict[str, Any]], test_branches: List[str]) -> None:
    """
    Clean up PRs and branches using parallel processing with retry logic.
    
    Args:
        repo: GitHub repository object
        prs_to_clean: List of PR information dictionaries
        test_branches: List of test branch names
    """
    print("\n" + "="*80)
    print("🧹 STARTING CLEANUP (with parallel processing)")
    print("="*80)
    
    # Thread-safe counters
    successful_operations = {"prs": 0, "branches": 0}
    failed_operations = {"prs": 0, "branches": 0}
    counter_lock = Lock()
    
    def update_counter(operation_type: str, success: bool):
        with counter_lock:
            if success:
                successful_operations[operation_type] += 1
            else:
                failed_operations[operation_type] += 1
    
    # Close PRs in parallel batches
    if prs_to_clean:
        print(f"\n📝 Closing {len(prs_to_clean)} PRs in parallel (batch size: 10)...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all PR closure tasks
            future_to_pr = {
                executor.submit(close_pr_with_retry, pr_info['pr_object']): pr_info
                for pr_info in prs_to_clean
            }
            
            # Process results as they complete
            for future in as_completed(future_to_pr):
                pr_info = future_to_pr[future]
                try:
                    result = future.result()
                    print(f"  ✅ {result['message']}")
                    update_counter("prs", True)
                except Exception as e:
                    error_msg = f"Error closing PR #{pr_info['number']}: {e}"
                    print(f"  ❌ {error_msg}")
                    update_counter("prs", False)
    
    # Collect all branches to delete
    branches_to_delete = set(test_branches)  # Start with standalone test branches
    
    # Add source branches from PRs to deletion list
    for pr_info in prs_to_clean:
        if pr_info['source_matches']:
            branches_to_delete.add(pr_info['source_branch'])
    
    # Delete branches in parallel batches
    if branches_to_delete:
        print(f"\n🌲 Deleting {len(branches_to_delete)} branches in parallel (batch size: 10)...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all branch deletion tasks
            future_to_branch = {
                executor.submit(delete_branch_with_retry, repo, branch_name): branch_name
                for branch_name in branches_to_delete
            }
            
            # Process results as they complete
            for future in as_completed(future_to_branch):
                branch_name = future_to_branch[future]
                try:
                    result = future.result()
                    print(f"  ✅ {result['message']}")
                    update_counter("branches", True)
                except github.GithubException as e:
                    if e.status == 404:
                        print(f"  ⚠️  Branch {branch_name} not found (may have been already deleted)")
                        update_counter("branches", True)  # Count as success since it's already gone
                    else:
                        print(f"  ❌ Error deleting branch {branch_name}: {e}")
                        update_counter("branches", False)
                except Exception as e:
                    print(f"  ❌ Unexpected error deleting branch {branch_name}: {e}")
                    update_counter("branches", False)
    
    # Print final summary
    print("\n" + "="*80)
    print("🎉 CLEANUP COMPLETED")
    print("="*80)
    print(f"📝 PRs: {successful_operations['prs']} successful, {failed_operations['prs']} failed")
    print(f"🌲 Branches: {successful_operations['branches']} successful, {failed_operations['branches']} failed")


def main():
    """Main function to run the cleanup script."""
    print("🚀 GitHub Test Branch and PR Cleanup Script (with Parallel Processing)")
    print("="*80)
    
    try:
        # Load environment variables
        print("📋 Loading environment configuration...")
        github_token, repo_url = load_environment()
        repo_name = parse_repo_name(repo_url)
        print(f"   Repository: {repo_name}")
        
        # Initialize GitHub client
        print("🔑 Authenticating with GitHub...")
        github_client = Github(github_token)
        repo = github_client.get_repo(repo_name)
        print(f"   ✅ Connected to repository: {repo.full_name}")
        print(f"   ⚡ Using parallel processing with batch size: 10")
        
        # Find PRs and branches to clean up
        prs_to_clean = get_prs_with_test_branches(repo)
        test_branches = get_all_test_branches(repo)
        
        # Display summary
        display_cleanup_summary(prs_to_clean, test_branches)
        
        # If nothing to clean up, exit
        if not prs_to_clean and not test_branches:
            print("\nExiting - nothing to clean up.")
            return
        
        # Get user confirmation
        if not get_user_confirmation():
            print("\nCleanup cancelled by user.")
            return
        
        # Perform cleanup
        cleanup_prs_and_branches(repo, prs_to_clean, test_branches)
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)
    except github.GithubException as e:
        print(f"❌ GitHub API error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
