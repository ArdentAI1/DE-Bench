#!/usr/bin/env python3
"""
Script to manage Astronomer de_bench_test_runner deployments.
This script can list existing de_bench_test_runner deployments and create additional ones
with proper sequential numbering.
"""

import subprocess
import json
import re
import sys
import argparse
import os
import time
import random
import functools
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

# Add parent directory to path to import utils
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.insert(0, parent_dir)

# Import parallel utilities
from utils import map_func


def retry_astro_command(max_retries: int = 3):
    """
    Retry decorator for astro CLI commands with exponential backoff.
    Includes automatic workspace switching fallback for workspace context errors.

    Args:
        max_retries: Maximum number of retries (default 3, so 4 total attempts)

    Returns:
        Decorated function
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (
                    subprocess.CalledProcessError,
                    EnvironmentError,
                    ValueError,
                ) as e:
                    last_exception = e
                    error_message = str(e).lower()

                    # Check stderr/stdout if it's a CalledProcessError
                    if hasattr(e, "stderr") and e.stderr:
                        error_message += " " + str(e.stderr).lower()
                    if hasattr(e, "stdout") and e.stdout:
                        error_message += " " + str(e.stdout).lower()

                    # Check for workspace context error and try to fix it
                    if (
                        "workspace context not set" in error_message
                        or "failed to find a valid workspace" in error_message
                    ):
                        try:
                            workspace_id = os.getenv("ASTRO_WORKSPACE_ID")
                            if workspace_id:
                                print(
                                    f"⚠️  Workspace context error detected, switching to workspace {workspace_id}"
                                )
                                subprocess.run(
                                    ["astro", "workspace", "switch", workspace_id],
                                    capture_output=True,
                                    text=True,
                                    check=True,
                                )
                                print(
                                    f"✅ Successfully switched to workspace {workspace_id}, retrying command..."
                                )
                                # Try the command again immediately after switching workspace
                                try:
                                    return func(*args, **kwargs)
                                except Exception:
                                    # If it still fails, continue with normal retry logic
                                    pass
                            else:
                                print(
                                    "⚠️  Workspace context error detected but ASTRO_WORKSPACE_ID not found"
                                )
                        except Exception as workspace_error:
                            print(f"⚠️  Failed to switch workspace: {workspace_error}")

                    if attempt == max_retries:
                        print(
                            f"❌ {func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

                    wait_time = 2**attempt
                    print(
                        f"⚠️  {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                    )
                    print(f"🔄 Retrying {func.__name__} in {wait_time} seconds...")
                    time.sleep(wait_time)

            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class AstroDeploymentManager:
    # Pattern for test runner deployment names
    TEST_RUNNER_PATTERN = "de_bench_test_runner"
    TEST_RUNNER_REGEX = re.compile(rf"^{TEST_RUNNER_PATTERN}_(\d+)$")

    def __init__(
        self,
        workspace_id: str = "cmcnpmwr80l9601lyycmaep42",
        auto_approve: bool = False,
    ):
        self.workspace_id = workspace_id
        self.auto_approve = auto_approve
        self.default_config = {
            "runtime_version": "13.1.0",
            "development_mode": "enable",
            "cloud_provider": "aws",
            "region": "us-east-1",
            "description": "This deployment is used for airflow tests and is hibernated when not in use.",
            "scheduler_size": "small",
        }

    @retry_astro_command(max_retries=3)
    def run_astro_command(
        self, command: List[str], exit_on_error: bool = True
    ) -> subprocess.CompletedProcess:
        """Run an astro CLI command and return the result with automatic retry and workspace switching."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return result
        except subprocess.CalledProcessError as e:
            if exit_on_error:
                print(f"Error running command: {' '.join(command)}")
                print(f"Error output: {e.stderr}")
                sys.exit(1)
            else:
                # Re-raise to let the decorator handle retries
                raise

    def list_deployments(self) -> List[Dict]:
        """List all deployments in the workspace."""
        command = ["astro", "deployment", "list", "--workspace-id", self.workspace_id]
        result = self.run_astro_command(command)

        # Parse the output to extract deployment information
        deployments = []
        lines = result.stdout.strip().split("\n")

        # Skip header lines and find data rows
        data_started = False
        for line in lines:
            if "---" in line or "NAME" in line:
                data_started = True
                continue

            if data_started and line.strip():
                # Split by whitespace, handling multiple spaces
                parts = [part for part in line.split() if part]
                if len(parts) >= 6:  # Ensure we have enough columns
                    deployment = {
                        "name": parts[0],
                        "namespace": parts[1],
                        "cluster": parts[2],
                        "cloud_provider": parts[3],
                        "region": parts[4],
                        "deployment_id": parts[5],
                    }
                    deployments.append(deployment)

        return deployments

    def get_test_runner_deployments(self) -> List[Dict]:
        """Get all deployments that match the de_bench_test_runner_X pattern."""
        all_deployments = self.list_deployments()

        test_runners = []
        for deployment in all_deployments:
            match = self.TEST_RUNNER_REGEX.match(deployment["name"])
            if match:
                deployment["runner_number"] = int(match.group(1))
                test_runners.append(deployment)

        # Sort by runner number
        test_runners.sort(key=lambda x: x["runner_number"])
        return test_runners

    def get_next_runner_number(self) -> int:
        """Get the next available de_bench_test_runner number, filling gaps first."""
        test_runners = self.get_test_runner_deployments()
        if not test_runners:
            return 1

        # Get all existing runner numbers and sort them
        existing_numbers = sorted([tr["runner_number"] for tr in test_runners])

        # Find the first gap in the sequence
        for i in range(1, max(existing_numbers) + 1):
            if i not in existing_numbers:
                return i

        # No gaps found, return the next number after the highest
        return max(existing_numbers) + 1

    def create_test_runner_deployment(
        self,
        runner_number: int,
        config: Optional[Dict] = None,
        max_retries: int = 5,
        max_wait: float = 10.0,
    ) -> tuple[bool, bool]:
        """Create a new de_bench_test_runner deployment with retry logic and exponential backoff.
        Returns (creation_success, hibernation_success)."""
        if config is None:
            config = self.default_config.copy()

        deployment_name = f"{self.TEST_RUNNER_PATTERN}_{runner_number}"

        command = [
            "astro",
            "deployment",
            "create",
            "--workspace-id",
            self.workspace_id,
            "--name",
            deployment_name,
            "--runtime-version",
            config["runtime_version"],
            "--development-mode",
            config["development_mode"],
            "--cloud-provider",
            config["cloud_provider"],
            "--region",
            config["region"],
            "-d",
            config["description"],
            "--scheduler-size",
            config["scheduler_size"],
        ]

        print(f"Creating deployment: {deployment_name}")

        for attempt in range(max_retries):
            result = self.run_astro_command(command, exit_on_error=False)

            # Check if the result is an exception (failed command)
            if isinstance(result, subprocess.CalledProcessError):
                error_msg = (
                    result.stderr.strip()
                    if result.stderr
                    else f"Command failed with exit code {result.returncode}"
                )

                if attempt < max_retries - 1:  # Not the last attempt
                    # Calculate wait time with exponential backoff + jitter
                    base_wait = min(2**attempt, max_wait)
                    jitter = random.uniform(0, 0.1 * base_wait)  # 10% jitter
                    wait_time = min(base_wait + jitter, max_wait)

                    print(
                        f"⚠️  Attempt {attempt + 1}/{max_retries} failed for {deployment_name}: {error_msg}"
                    )
                    print(f"🔄 Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(
                        f"❌ Failed to create {deployment_name} after {max_retries} attempts: {error_msg}"
                    )
                    return False, False
            else:
                print(
                    f"✅ Successfully created {deployment_name}"
                    + (f" (attempt {attempt + 1})" if attempt > 0 else "")
                )
                if result.stdout:
                    print(f"Output: {result.stdout}")

                # Set required variables immediately after creation (never during wake)
                self.set_deployment_variables(deployment_name)

                # Immediately hibernate the deployment after creation
                hibernate_success = self.hibernate_deployment(deployment_name)
                if not hibernate_success:
                    print(
                        f"⚠️  WARNING: {deployment_name} was created but failed to hibernate!"
                    )
                    print(
                        f"    You may need to manually hibernate this deployment to avoid costs."
                    )

                return True, hibernate_success

        return False, False

    def hibernate_deployment(
        self, deployment_name: str, max_retries: int = 3, max_wait: float = 5.0
    ) -> bool:
        """Hibernate a deployment with retry logic and exponential backoff."""
        command = [
            "astro",
            "deployment",
            "hibernate",
            "--deployment-name",
            deployment_name,
            "-f",  # Force without confirmation
        ]

        print(f"🛌 Hibernating deployment: {deployment_name}")

        for attempt in range(max_retries):
            result = self.run_astro_command(command, exit_on_error=False)

            # Check if the result is an exception (failed command)
            if isinstance(result, subprocess.CalledProcessError):
                error_msg = (
                    result.stderr.strip()
                    if result.stderr
                    else f"Command failed with exit code {result.returncode}"
                )

                if attempt < max_retries - 1:  # Not the last attempt
                    # Calculate wait time with exponential backoff + jitter
                    base_wait = min(2**attempt, max_wait)
                    jitter = random.uniform(0, 0.1 * base_wait)  # 10% jitter
                    wait_time = min(base_wait + jitter, max_wait)

                    print(
                        f"⚠️  Hibernation attempt {attempt + 1}/{max_retries} failed for {deployment_name}: {error_msg}"
                    )
                    print(f"🔄 Retrying hibernation in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(
                        f"❌ Failed to hibernate {deployment_name} after {max_retries} attempts: {error_msg}"
                    )
                    return False
            else:
                print(
                    f"✅ Successfully hibernated {deployment_name}"
                    + (f" (attempt {attempt + 1})" if attempt > 0 else "")
                )
                return True

        return False

    def set_deployment_variables(self, deployment_name: str) -> None:
        """Create/update required Airflow variables on the deployment. Called only on creation."""
        username = os.getenv("AIRFLOW_USERNAME", "airflow")
        password = os.getenv("AIRFLOW_PASSWORD", "airflow")

        commands: List[List[str]] = [
            [
                "astro",
                "deployment",
                "variable",
                "create",
                "_AIRFLOW_WWW_USER_CREATE=true",
                "--deployment-name",
                deployment_name,
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                f"_AIRFLOW_WWW_USER_USERNAME={username}",
                "--deployment-name",
                deployment_name,
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                f"_AIRFLOW_WWW_USER_PASSWORD={password}",
                "--deployment-name",
                deployment_name,
                "-s",
            ],
            [
                "astro",
                "deployment",
                "variable",
                "create",
                "AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth",
                "--deployment-name",
                deployment_name,
            ],
        ]

        # Optional secret
        slack_app_url = os.getenv("SLACK_APP_URL")
        if slack_app_url:
            commands.append(
                [
                    "astro",
                    "deployment",
                    "variable",
                    "create",
                    f"SLACK_APP_URL={slack_app_url}",
                    "--deployment-name",
                    deployment_name,
                    "-s",
                ]
            )

        for cmd in commands:
            var_name = cmd[4].split("=")[0]
            try:
                res = self.run_astro_command(cmd, exit_on_error=False)
                # If it failed, check if it's due to already existing variable; otherwise show error
                if isinstance(res, subprocess.CalledProcessError):
                    err = (res.stderr or "").lower()
                    if "already exists" in err or "conflict" in err:
                        print(
                            f"ℹ️  Variable {var_name} already exists for {deployment_name}, skipping"
                        )
                    else:
                        print(
                            f"⚠️  Failed to create variable {var_name} for {deployment_name}: {res.stderr}"
                        )
                else:
                    print(f"✅ Set variable {var_name} for {deployment_name}")
            except Exception as e:
                print(f"⚠️  Unexpected error setting variable {var_name}: {e}")

    def get_next_available_numbers(self, count: int) -> List[int]:
        """Get the next N available runner numbers, filling gaps first."""
        test_runners = self.get_test_runner_deployments()
        existing_numbers = set([tr["runner_number"] for tr in test_runners])

        available_numbers = []
        current = 1

        while len(available_numbers) < count:
            if current not in existing_numbers:
                available_numbers.append(current)
            current += 1

        return available_numbers

    def create_multiple_test_runners(self, count: int) -> tuple[List[str], List[str]]:
        """Create multiple de_bench_test_runner deployments in parallel, filling gaps first.
        Returns (created_deployments, hibernation_failed_deployments)."""
        runner_numbers = self.get_next_available_numbers(count)

        print(
            f"Creating {count} new {self.TEST_RUNNER_PATTERN} deployments in parallel"
        )
        print(f"Numbers to create: {', '.join(map(str, runner_numbers))}")

        # Create wrapper function for parallel processing
        def create_single_runner(runner_number: int) -> Dict:
            """Create a single test runner and return result info."""
            creation_success, hibernation_success = self.create_test_runner_deployment(
                runner_number
            )
            return {
                "runner_number": runner_number,
                "name": f"{self.TEST_RUNNER_PATTERN}_{runner_number}",
                "creation_success": creation_success,
                "hibernation_success": hibernation_success,
            }

        # Process all creations in parallel
        results = map_func(create_single_runner, runner_numbers)

        # Extract successful deployments and track hibernation failures
        created_deployments = []
        hibernation_failed = []

        for result in results:
            if result["creation_success"]:
                created_deployments.append(result["name"])
                if not result["hibernation_success"]:
                    hibernation_failed.append(result["name"])
            else:
                print(
                    f"⚠️  Failed to create {result['name']}, continuing with remaining deployments..."
                )

        return created_deployments, hibernation_failed

    def display_test_runners(self):
        """Display all existing de_bench_test_runner deployments with status information."""
        test_runners = self.get_test_runner_deployments()

        if not test_runners:
            print(f"No {self.TEST_RUNNER_PATTERN} deployments found.")
            return

        print(f"\nFound {len(test_runners)} {self.TEST_RUNNER_PATTERN} deployment(s):")
        print(f"🔄 Getting status information in parallel...")

        def get_deployment_status(deployment: Dict) -> Dict:
            """Get status information for a single deployment."""
            details = self.get_deployment_details(deployment["name"])
            if details:
                metadata = details.get("metadata", {})
                status = metadata.get("status", "UNKNOWN")
                updated_at_str = metadata.get("updated_at")

                # Calculate status duration
                if updated_at_str:
                    try:
                        updated_at = datetime.fromisoformat(
                            updated_at_str.replace("Z", "+00:00")
                        )
                        current_time = datetime.now(timezone.utc)
                        status_duration_hours = (
                            current_time - updated_at
                        ).total_seconds() / 3600
                    except ValueError:
                        status_duration_hours = None
                else:
                    status_duration_hours = None

                deployment["status"] = status
                deployment["status_duration_hours"] = status_duration_hours
            else:
                deployment["status"] = "UNKNOWN"
                deployment["status_duration_hours"] = None

            return deployment

        # Get status for all deployments in parallel
        test_runners_with_status = map_func(get_deployment_status, test_runners)

        print("=" * 110)
        print(
            f"{'NAME':<30} {'REGION':<15} {'STATUS':<20} {'STATUS DURATION':<15} {'DEPLOYMENT ID'}"
        )
        print("-" * 110)

        for tr in test_runners_with_status:
            status = tr.get("status", "UNKNOWN")

            # Status emoji
            status_emoji = {
                "HIBERNATING": "🛌",
                "HEALTHY": "✅",
                "UNHEALTHY": "❌",
                "DEPLOYING": "🚀",
                "DELETING": "🗑️",
                "UNKNOWN": "❓",
            }.get(status, "📊")

            # Format status duration
            if tr.get("status_duration_hours") is not None:
                if tr["status_duration_hours"] < 1:
                    duration_str = f"{tr['status_duration_hours'] * 60:.0f}m"
                else:
                    duration_str = f"{tr['status_duration_hours']:.1f}h"
            else:
                duration_str = "Unknown"

            status_display = f"{status_emoji} {status}"

            print(
                f"{tr['name']:<30} {tr['region']:<15} {status_display:<20} {duration_str:<15} {tr['deployment_id']}"
            )

    def delete_deployment(
        self, deployment_id: str, deployment_name: str
    ) -> tuple[bool, str]:
        """Delete a deployment by ID. Returns (success, error_message)."""
        command = [
            "astro",
            "deployment",
            "delete",
            deployment_id,
            "--force",  # Skip confirmation in CLI
        ]

        print(f"Deleting deployment: {deployment_name} ({deployment_id})")
        result = self.run_astro_command(command, exit_on_error=False)

        # Check if the result is an exception (failed command)
        if isinstance(result, subprocess.CalledProcessError):
            error_msg = (
                result.stderr.strip()
                if result.stderr
                else f"Command failed with exit code {result.returncode}"
            )
            print(f"❌ Failed to delete {deployment_name}: {error_msg}")
            return False, error_msg
        else:
            print(f"✅ Successfully deleted {deployment_name}")
            return True, ""

    def delete_all_test_runners(self) -> tuple[List[str], List[Dict]]:
        """Delete all de_bench_test_runner deployments with confirmation. Returns (successful_deletions, failed_deletions)."""
        test_runners = self.get_test_runner_deployments()

        if not test_runners:
            print(f"No {self.TEST_RUNNER_PATTERN} deployments found to delete.")
            return [], []

        print(
            f"\n⚠️  Found {len(test_runners)} {self.TEST_RUNNER_PATTERN} deployment(s) to delete:"
        )
        print("=" * 80)
        for tr in test_runners:
            print(f"  - {tr['name']} ({tr['deployment_id']})")

        print("\n🚨 WARNING: This action cannot be undone!")

        if not self.auto_approve:
            confirm1 = input(
                f"Are you sure you want to delete ALL {self.TEST_RUNNER_PATTERN} deployments? (type 'yes' to confirm): "
            )

            if confirm1.lower() != "yes":
                print("❌ Deletion cancelled.")
                return [], []

            confirm2 = input(
                f"Final confirmation: Delete {len(test_runners)} deployments? (type 'DELETE' to confirm): "
            )

            if confirm2 != "DELETE":
                print("❌ Deletion cancelled.")
                return [], []
        else:
            print("✅ Auto-approve enabled - skipping confirmation prompts")

        print(
            f"\n🗑️  Deleting {len(test_runners)} {self.TEST_RUNNER_PATTERN} deployments in parallel..."
        )

        # Create a wrapper function for parallel processing
        def delete_single_deployment(deployment_info: Dict) -> Dict:
            """Delete a single deployment and return result info."""
            success, error_msg = self.delete_deployment(
                deployment_info["deployment_id"], deployment_info["name"]
            )

            return {
                "name": deployment_info["name"],
                "deployment_id": deployment_info["deployment_id"],
                "success": success,
                "error": error_msg,
            }

        # Process all deletions in parallel
        results = map_func(delete_single_deployment, test_runners)

        # Separate successful and failed deletions
        deleted_deployments = []
        failed_deletions = []

        for result in results:
            if result["success"]:
                deleted_deployments.append(result["name"])
            else:
                failed_deletions.append(
                    {
                        "name": result["name"],
                        "deployment_id": result["deployment_id"],
                        "error": result["error"],
                    }
                )

        return deleted_deployments, failed_deletions

    def get_non_test_runner_deployments(self) -> List[Dict]:
        """Get all deployments that do NOT match the de_bench_test_runner_X pattern."""
        all_deployments = self.list_deployments()

        non_test_runners = []
        for deployment in all_deployments:
            # If it doesn't match the test runner pattern, include it
            if not self.TEST_RUNNER_REGEX.match(deployment["name"]):
                non_test_runners.append(deployment)

        # Sort by name for consistent output
        non_test_runners.sort(key=lambda x: x["name"])
        return non_test_runners

    def delete_all_non_test_runners(self) -> tuple[List[str], List[Dict]]:
        """Delete all deployments that do NOT match de_bench_test_runner pattern. Returns (successful_deletions, failed_deletions)."""
        non_test_runners = self.get_non_test_runner_deployments()

        if not non_test_runners:
            print(f"No non-{self.TEST_RUNNER_PATTERN} deployments found to delete.")
            return [], []

        print(
            f"\n⚠️  Found {len(non_test_runners)} deployment(s) that do NOT match the {self.TEST_RUNNER_PATTERN} pattern:"
        )
        print("=" * 80)
        for deployment in non_test_runners:
            print(f"  - {deployment['name']} ({deployment['deployment_id']})")

        print(
            f"\n🚨 WARNING: This will delete ALL deployments that are NOT {self.TEST_RUNNER_PATTERN} deployments!"
        )
        print("🚨 This action cannot be undone!")

        if not self.auto_approve:
            confirm1 = input(
                f"Are you sure you want to delete ALL non-{self.TEST_RUNNER_PATTERN} deployments? (type 'yes' to confirm): "
            )

            if confirm1.lower() != "yes":
                print("❌ Deletion cancelled.")
                return [], []

            confirm2 = input(
                f"Final confirmation: Delete {len(non_test_runners)} non-test-runner deployments? (type 'DELETE' to confirm): "
            )

            if confirm2 != "DELETE":
                print("❌ Deletion cancelled.")
                return [], []
        else:
            print("✅ Auto-approve enabled - skipping confirmation prompts")

        print(
            f"\n🗑️  Deleting {len(non_test_runners)} non-{self.TEST_RUNNER_PATTERN} deployments in parallel..."
        )

        # Create a wrapper function for parallel processing
        def delete_single_deployment(deployment_info: Dict) -> Dict:
            """Delete a single deployment and return result info."""
            success, error_msg = self.delete_deployment(
                deployment_info["deployment_id"], deployment_info["name"]
            )

            return {
                "name": deployment_info["name"],
                "deployment_id": deployment_info["deployment_id"],
                "success": success,
                "error": error_msg,
            }

        # Process all deletions in parallel
        results = map_func(delete_single_deployment, non_test_runners)

        # Separate successful and failed deletions
        deleted_deployments = []
        failed_deletions = []

        for result in results:
            if result["success"]:
                deleted_deployments.append(result["name"])
            else:
                failed_deletions.append(
                    {
                        "name": result["name"],
                        "deployment_id": result["deployment_id"],
                        "error": result["error"],
                    }
                )

        return deleted_deployments, failed_deletions

    def get_deployment_details(self, deployment_name: str) -> Optional[Dict]:
        """Get detailed deployment information including timestamps."""
        command = [
            "astro",
            "deployment",
            "inspect",
            "--deployment-name",
            deployment_name,
            "--output",
            "json",
            "--workspace-id",
            self.workspace_id,
        ]

        result = self.run_astro_command(command, exit_on_error=False)

        # Check if the result is an exception (failed command)
        if isinstance(result, subprocess.CalledProcessError):
            print(f"⚠️  Failed to get details for {deployment_name}: {result.stderr}")
            return None

        try:
            data = json.loads(result.stdout)
            return data.get("deployment", {})
        except json.JSONDecodeError as e:
            print(f"⚠️  Failed to parse deployment details for {deployment_name}: {e}")
            return None

    def get_deployments_older_than_hour(self) -> List[Dict]:
        """Get all HEALTHY de_bench_test_runner deployments that haven't been modified in over 1 hour."""
        test_runners = self.get_test_runner_deployments()
        current_time = datetime.now(timezone.utc)
        one_hour_ago = current_time - timedelta(hours=1)

        print(
            f"🕒 Checking {len(test_runners)} deployments in parallel for staleness (HEALTHY & last modified before {one_hour_ago.strftime('%Y-%m-%d %H:%M:%S UTC')})"
        )

        def check_deployment_age(deployment: Dict) -> Optional[Dict]:
            """Check a single deployment's staleness and return deployment with age info if stale enough."""
            details = self.get_deployment_details(deployment["name"])
            if not details:
                print(f"⚠️  Failed to get details for {deployment['name']}")
                return None

            metadata = details.get("metadata", {})
            created_at_str = metadata.get("created_at")
            updated_at_str = metadata.get("updated_at")
            status = metadata.get("status", "UNKNOWN")

            if not updated_at_str:
                print(
                    f"⚠️  No last modified timestamp found for {deployment['name']}, skipping"
                )
                return None

            try:
                # Parse the timestamps (format: "2025-09-23T20:33:59.556Z")
                updated_at = datetime.fromisoformat(
                    updated_at_str.replace("Z", "+00:00")
                )

                # Calculate staleness (time since last modification)
                deployment["updated_at"] = updated_at
                deployment["staleness_hours"] = (
                    current_time - updated_at
                ).total_seconds() / 3600
                deployment["status"] = status
                deployment["status_duration_hours"] = deployment[
                    "staleness_hours"
                ]  # Same as staleness for this purpose

                # Also parse created_at for display purposes
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(
                            created_at_str.replace("Z", "+00:00")
                        )
                        deployment["created_at"] = created_at
                        deployment["age_hours"] = (
                            current_time - created_at
                        ).total_seconds() / 3600
                    except ValueError:
                        deployment["created_at"] = None
                        deployment["age_hours"] = None

                # Create status emoji
                status_emoji = {
                    "HIBERNATING": "🛌",
                    "HEALTHY": "✅",
                    "UNHEALTHY": "❌",
                    "DEPLOYING": "🚀",
                    "DELETING": "🗑️",
                    "UNKNOWN": "❓",
                }.get(status, "📊")

                # Format status duration (staleness)
                if deployment["staleness_hours"] < 1:
                    status_duration_str = f"{deployment['staleness_hours'] * 60:.0f}m"
                else:
                    status_duration_str = f"{deployment['staleness_hours']:.1f}h"
                status_info = f"{status_emoji} {status} ({status_duration_str})"

                # Use last modified time for filtering (not creation time) AND only include HEALTHY deployments
                if updated_at < one_hour_ago and status == "HEALTHY":
                    print(
                        f"🔄 {deployment['name']}: last modified {deployment['staleness_hours']:.1f}h ago | {status_info} (STALE & HEALTHY)"
                    )
                    return deployment
                elif updated_at < one_hour_ago and status != "HEALTHY":
                    print(
                        f"🛌 {deployment['name']}: last modified {deployment['staleness_hours']:.1f}h ago | {status_info} (stale but not healthy - skipping)"
                    )
                    return None
                else:
                    staleness_minutes = (current_time - updated_at).total_seconds() / 60
                    print(
                        f"🆕 {deployment['name']}: last modified {staleness_minutes:.0f}m ago | {status_info} (too recent)"
                    )
                    return None

            except ValueError as e:
                print(f"⚠️  Failed to parse timestamp for {deployment['name']}: {e}")
                return None

        # Check all deployments in parallel
        results = map_func(check_deployment_age, test_runners)

        # Filter out None results to get only old deployments
        old_deployments = [result for result in results if result is not None]

        print(
            f"✅ Staleness check complete: found {len(old_deployments)} HEALTHY deployment(s) not modified in over 1 hour"
        )
        return old_deployments

    def recreate_old_deployments(self) -> tuple[List[str], List[str], List[str]]:
        """Recreate all HEALTHY de_bench_test_runner deployments that haven't been modified in over 1 hour.
        Returns (deleted_deployments, created_deployments, hibernation_failed_deployments)."""
        old_deployments = self.get_deployments_older_than_hour()

        if not old_deployments:
            print(
                f"✅ No HEALTHY {self.TEST_RUNNER_PATTERN} deployments found that haven't been modified in over 1 hour."
            )
            return [], [], []

        print(
            f"\n⚠️  Found {len(old_deployments)} HEALTHY {self.TEST_RUNNER_PATTERN} deployment(s) not modified in over 1 hour:"
        )
        print("=" * 100)
        for deployment in old_deployments:
            staleness_str = f"{deployment['staleness_hours']:.1f}h ago"
            status = deployment.get("status", "UNKNOWN")

            # Status emoji and duration
            status_emoji = {
                "HIBERNATING": "🛌",
                "HEALTHY": "✅",
                "UNHEALTHY": "❌",
                "DEPLOYING": "🚀",
                "DELETING": "🗑️",
                "UNKNOWN": "❓",
            }.get(status, "📊")

            if deployment.get("status_duration_hours") is not None:
                if deployment["status_duration_hours"] < 1:
                    status_duration_str = (
                        f"{deployment['status_duration_hours'] * 60:.0f}m"
                    )
                else:
                    status_duration_str = f"{deployment['status_duration_hours']:.1f}h"
                status_info = f"{status_emoji} {status} ({status_duration_str})"
            else:
                status_info = f"{status_emoji} {status}"

            print(
                f"  - {deployment['name']} | last modified {staleness_str} | {status_info}"
            )

        print(f"\n🚨 WARNING: This will DELETE and RECREATE these deployments!")
        print(
            "🚨 This action cannot be undone! All DAGs and task history will be lost!"
        )

        if not self.auto_approve:
            confirm1 = input(
                f"Are you sure you want to recreate ALL stale HEALTHY {self.TEST_RUNNER_PATTERN} deployments? (type 'yes' to confirm): "
            )

            if confirm1.lower() != "yes":
                print("❌ Recreation cancelled.")
                return [], [], []

            confirm2 = input(
                f"Final confirmation: Recreate {len(old_deployments)} stale HEALTHY deployments? (type 'RECREATE' to confirm): "
            )

            if confirm2 != "RECREATE":
                print("❌ Recreation cancelled.")
                return [], [], []
        else:
            print("✅ Auto-approve enabled - skipping confirmation prompts")

        print(
            f"\n🔄 Recreating {len(old_deployments)} stale {self.TEST_RUNNER_PATTERN} deployments..."
        )

        # Step 1: Delete stale deployments
        print(f"\n🗑️  Step 1: Deleting {len(old_deployments)} stale deployments...")

        def delete_single_deployment(deployment_info: Dict) -> Dict:
            """Delete a single deployment and return result info."""
            success, error_msg = self.delete_deployment(
                deployment_info["deployment_id"], deployment_info["name"]
            )
            return {
                "name": deployment_info["name"],
                "deployment_id": deployment_info["deployment_id"],
                "runner_number": deployment_info["runner_number"],
                "success": success,
                "error": error_msg,
            }

        # Delete in parallel
        delete_results = map_func(delete_single_deployment, old_deployments)

        # Check deletion results
        deleted_deployments = []
        failed_deletions = []
        runner_numbers_to_recreate = []

        for result in delete_results:
            if result["success"]:
                deleted_deployments.append(result["name"])
                runner_numbers_to_recreate.append(result["runner_number"])
            else:
                failed_deletions.append(result)

        if failed_deletions:
            print(
                f"\n❌ Failed to delete {len(failed_deletions)} deployment(s). Aborting recreation."
            )
            for failure in failed_deletions:
                print(f"  - {failure['name']}: {failure['error']}")
            return deleted_deployments, [], []

        print(f"✅ Successfully deleted {len(deleted_deployments)} deployment(s)")

        # Step 2: Recreate the deployments with the same numbers
        print(
            f"\n🏗️  Step 2: Recreating {len(runner_numbers_to_recreate)} deployments..."
        )

        def create_single_runner(runner_number: int) -> Dict:
            """Create a single test runner and return result info."""
            creation_success, hibernation_success = self.create_test_runner_deployment(
                runner_number
            )
            return {
                "runner_number": runner_number,
                "name": f"{self.TEST_RUNNER_PATTERN}_{runner_number}",
                "creation_success": creation_success,
                "hibernation_success": hibernation_success,
            }

        # Create in parallel
        create_results = map_func(create_single_runner, runner_numbers_to_recreate)

        # Extract results
        created_deployments = []
        hibernation_failed = []

        for result in create_results:
            if result["creation_success"]:
                created_deployments.append(result["name"])
                if not result["hibernation_success"]:
                    hibernation_failed.append(result["name"])
            else:
                print(f"⚠️  Failed to recreate {result['name']}")

        return deleted_deployments, created_deployments, hibernation_failed


def main():
    """Main function to handle command line interaction."""
    parser = argparse.ArgumentParser(
        description="Manage Astronomer de_bench_test_runner deployments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Interactive mode to create deployments
  %(prog)s --delete-all       # Delete all de_bench_test_runner deployments
  %(prog)s --delete-others    # Delete all non-de_bench_test_runner deployments
  %(prog)s --recreate-all     # Delete all de_bench_test_runner deployments and recreate (interactive)
  %(prog)s --recreate-all 5   # Delete all de_bench_test_runner deployments and recreate 5 new ones
  %(prog)s --recreate-old     # Recreate HEALTHY deployments not modified in over 1 hour
  %(prog)s --recreate-old -y  # Auto-approve recreation without confirmation prompts
        """,
    )
    parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete all de_bench_test_runner deployments (requires confirmation)",
    )
    parser.add_argument(
        "--delete-others",
        action="store_true",
        help="Delete all deployments that do NOT follow the de_bench_test_runner pattern (requires confirmation)",
    )
    parser.add_argument(
        "--recreate-all",
        type=int,
        nargs="?",
        const=-1,
        metavar="COUNT",
        help="Delete all de_bench_test_runner deployments and recreate them. Optionally specify COUNT (default: interactive)",
    )
    parser.add_argument(
        "--recreate-old",
        action="store_true",
        help="Recreate HEALTHY de_bench_test_runner deployments that haven't been modified in over 1 hour (requires confirmation)",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Auto-approve all confirmations (skip user prompts)",
    )

    args = parser.parse_args()
    manager = AstroDeploymentManager(auto_approve=args.yes)

    print("🚀 Astronomer Test Runner Deployment Manager")
    print("=" * 50)

    # Check for conflicting arguments
    exclusive_args = [
        args.delete_all,
        args.delete_others,
        args.recreate_all is not None,
        args.recreate_old,
    ]
    if sum(exclusive_args) > 1:
        print("❌ Cannot use multiple action arguments at the same time.")
        print(
            "   Choose one of: --delete-all, --delete-others, --recreate-all, or --recreate-old"
        )
        sys.exit(1)

    if args.delete_all:
        # Delete all test_runner deployments
        try:
            deleted, failed = manager.delete_all_test_runners()

            # Print summary
            print("\n" + "=" * 80)
            print("🔥 DELETION SUMMARY")
            print("=" * 80)

            if deleted:
                print(f"\n✅ Successfully deleted {len(deleted)} deployment(s):")
                for deployment in deleted:
                    print(f"  - {deployment}")

            if failed:
                print(f"\n❌ Failed to delete {len(failed)} deployment(s):")
                for failure in failed:
                    print(f"  - {failure['name']} ({failure['deployment_id']})")
                    print(f"    Reason: {failure['error']}")

            if not deleted and not failed:
                print("\n❌ No deployments were processed.")

            # Overall result
            total_attempted = len(deleted) + len(failed)
            if total_attempted > 0:
                success_rate = (len(deleted) / total_attempted) * 100
                print(
                    f"\n📊 Overall: {len(deleted)}/{total_attempted} successful ({success_rate:.1f}%)"
                )

        except KeyboardInterrupt:
            print("\n\n👋 Cancelled by user.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")

    elif args.delete_others:
        # Delete all non-test_runner deployments
        try:
            deleted, failed = manager.delete_all_non_test_runners()

            # Print summary
            print("\n" + "=" * 80)
            print("🔥 DELETION SUMMARY")
            print("=" * 80)

            if deleted:
                print(
                    f"\n✅ Successfully deleted {len(deleted)} non-test-runner deployment(s):"
                )
                for deployment in deleted:
                    print(f"  - {deployment}")

            if failed:
                print(f"\n❌ Failed to delete {len(failed)} deployment(s):")
                for failure in failed:
                    print(f"  - {failure['name']} ({failure['deployment_id']})")
                    print(f"    Reason: {failure['error']}")

            if not deleted and not failed:
                print("\n❌ No deployments were processed.")

            # Overall result
            total_attempted = len(deleted) + len(failed)
            if total_attempted > 0:
                success_rate = (len(deleted) / total_attempted) * 100
                print(
                    f"\n📊 Overall: {len(deleted)}/{total_attempted} successful ({success_rate:.1f}%)"
                )

        except KeyboardInterrupt:
            print("\n\n👋 Cancelled by user.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")

    elif args.recreate_all is not None:
        # Delete all test_runner deployments and recreate them
        try:
            # First, show what exists
            print("🔄 RECREATE ALL TEST RUNNERS")
            print("=" * 50)
            manager.display_test_runners()

            # Delete all existing test runners
            print(
                f"\n🗑️  Step 1: Deleting all existing {manager.TEST_RUNNER_PATTERN} deployments..."
            )
            deleted, failed = manager.delete_all_test_runners()

            if failed:
                print(
                    f"\n❌ Failed to delete {len(failed)} deployment(s). Aborting recreation."
                )
                print("   Fix deletion issues before retrying recreation.")
                for failure in failed:
                    print(f"  - {failure['name']}: {failure['error']}")
                sys.exit(1)

            if not deleted:
                print("ℹ️  No existing deployments found to delete.")
            else:
                print(f"✅ Successfully deleted {len(deleted)} deployment(s)")

            # Determine how many to recreate
            if args.recreate_all == -1:
                # Interactive mode
                print(
                    f"\n🏗️  Step 2: Creating new {manager.TEST_RUNNER_PATTERN} deployments..."
                )
                try:
                    count_input = input(
                        f"How many new {manager.TEST_RUNNER_PATTERN} deployments would you like to create? (0 to skip): "
                    )
                    count = int(count_input.strip())
                except ValueError:
                    print("❌ Invalid input. Exiting.")
                    sys.exit(1)
            else:
                # Count provided via argument
                count = args.recreate_all
                print(
                    f"\n🏗️  Step 2: Creating {count} new {manager.TEST_RUNNER_PATTERN} deployments..."
                )

            if count <= 0:
                print("No new deployments to create. Recreation complete.")
                return

            if count > 10 and not manager.auto_approve:
                confirm = input(
                    f"You're about to create {count} deployments. Are you sure? (y/N): "
                )
                if confirm.lower() != "y":
                    print("Cancelled.")
                    return

            # Create the new deployments
            created, hibernation_failed = manager.create_multiple_test_runners(count)

            # Print final summary
            print("\n" + "=" * 80)
            print("🔄 RECREATION SUMMARY")
            print("=" * 80)

            if deleted:
                print(f"\n🗑️  Deleted: {len(deleted)} deployment(s)")

            if created:
                print(f"\n✅ Created: {len(created)} deployment(s)")
                for deployment in created:
                    print(f"  - {deployment}")

            if len(created) < count:
                print(
                    f"\n⚠️  Only {len(created)} out of {count} requested deployments were created due to errors."
                )

            # Show hibernation failure summary if any
            if hibernation_failed:
                print(f"\n🚨 HIBERNATION FAILURES - MANUAL ACTION REQUIRED!")
                print("=" * 60)
                print(
                    f"The following {len(hibernation_failed)} deployment(s) were created but failed to hibernate:"
                )
                for deployment in hibernation_failed:
                    print(f"  ⚠️  {deployment}")
                print(
                    "\n💡 To avoid costs, manually hibernate these deployments using:"
                )
                print(
                    "   astro deployment hibernate --deployment-name <DEPLOYMENT_NAME> -f"
                )
                print("   Or use the Astronomer UI to hibernate them.")
            elif created:
                print(f"\n✅ All {len(created)} deployments successfully hibernated!")

            print(f"\n🎉 Recreation complete! Total active deployments: {len(created)}")

        except KeyboardInterrupt:
            print("\n\n👋 Cancelled by user.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")

    elif args.recreate_old:
        # Recreate deployments older than 1 hour
        try:
            deleted, created, hibernation_failed = manager.recreate_old_deployments()

            # Print summary
            print("\n" + "=" * 80)
            print("🔄 RECREATION SUMMARY")
            print("=" * 80)

            if deleted:
                print(f"\n🗑️  Deleted: {len(deleted)} deployment(s)")
                for deployment in deleted:
                    print(f"  - {deployment}")

            if created:
                print(f"\n✅ Created: {len(created)} deployment(s)")
                for deployment in created:
                    print(f"  - {deployment}")

            if not deleted and not created:
                print("\n❌ No deployments were processed.")

            # Show hibernation failure summary if any
            if hibernation_failed:
                print(f"\n🚨 HIBERNATION FAILURES - MANUAL ACTION REQUIRED!")
                print("=" * 60)
                print(
                    f"The following {len(hibernation_failed)} deployment(s) were created but failed to hibernate:"
                )
                for deployment in hibernation_failed:
                    print(f"  ⚠️  {deployment}")
                print(
                    "\n💡 To avoid costs, manually hibernate these deployments using:"
                )
                print(
                    "   astro deployment hibernate --deployment-name <DEPLOYMENT_NAME> -f"
                )
                print("   Or use the Astronomer UI to hibernate them.")
            elif created:
                print(f"\n✅ All {len(created)} deployments successfully hibernated!")

            # Overall result
            if created and deleted:
                print(
                    f"\n🎉 Recreation complete! Successfully recreated {len(created)} deployment(s)"
                )

        except KeyboardInterrupt:
            print("\n\n👋 Cancelled by user.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")

    else:
        # Interactive mode to create deployments
        # Display existing test runners
        manager.display_test_runners()

        # Get user input for how many new deployments to create
        try:
            # Show preview of what numbers would be created
            print(f"\nNext available numbers (filling gaps first):")
            preview_numbers = manager.get_next_available_numbers(
                10
            )  # Show first 10 available
            print(
                f"  {', '.join(map(str, preview_numbers[:5]))}"
                + ("..." if len(preview_numbers) > 5 else "")
            )

            count_input = input(
                f"\nHow many new {manager.TEST_RUNNER_PATTERN} deployments would you like to create? (0 to exit): "
            )
            count = int(count_input.strip())

            if count <= 0:
                print("No deployments to create. Exiting.")
                return

            if count > 10 and not manager.auto_approve:
                confirm = input(
                    f"You're about to create {count} deployments. Are you sure? (y/N): "
                )
                if confirm.lower() != "y":
                    print("Cancelled.")
                    return

            # Create the deployments
            created, hibernation_failed = manager.create_multiple_test_runners(count)

            print(f"\n✅ Successfully created {len(created)} deployment(s):")
            for deployment in created:
                print(f"  - {deployment}")

            if len(created) < count:
                print(
                    f"\n⚠️  Only {len(created)} out of {count} deployments were created due to errors."
                )

            # Show hibernation failure summary if any
            if hibernation_failed:
                print(f"\n🚨 HIBERNATION FAILURES - MANUAL ACTION REQUIRED!")
                print("=" * 60)
                print(
                    f"The following {len(hibernation_failed)} deployment(s) were created but failed to hibernate:"
                )
                for deployment in hibernation_failed:
                    print(f"  ⚠️  {deployment}")
                print(
                    "\n💡 To avoid costs, manually hibernate these deployments using:"
                )
                print(
                    "   astro deployment hibernate --deployment-name <DEPLOYMENT_NAME> -f"
                )
                print("   Or use the Astronomer UI to hibernate them.")
            elif created:
                print(f"\n✅ All {len(created)} deployments successfully hibernated!")

        except ValueError:
            print("❌ Invalid input. Please enter a valid number.")
        except KeyboardInterrupt:
            print("\n\n👋 Cancelled by user.")
        except Exception as e:
            print(f"❌ An error occurred: {e}")


if __name__ == "__main__":
    main()
