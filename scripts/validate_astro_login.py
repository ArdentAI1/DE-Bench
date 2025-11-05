#!/usr/bin/env python3
"""
Astro Login Validation Helper Script

This script validates that Astro CLI is properly authenticated using the
ASTRO_API_TOKEN environment variable from .env file.
"""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


def load_env_file():
    """Load environment variables from .env file."""
    # Look for .env file in current directory or parent directories
    current_dir = Path.cwd()
    env_file = None

    # Check current directory first
    if (current_dir / ".env").exists():
        env_file = current_dir / ".env"
    else:
        # Check parent directories up to 3 levels
        for parent in current_dir.parents[:3]:
            if (parent / ".env").exists():
                env_file = parent / ".env"
                break

    if env_file:
        print(f"Loading environment from: {env_file}")
        load_dotenv(env_file)
        return True
    else:
        print("Warning: No .env file found in current directory or parent directories")
        return False


def check_astro_token():
    """Check if ASTRO_API_TOKEN is available."""
    token = os.getenv("ASTRO_API_TOKEN")
    if not token:
        print("❌ ASTRO_API_TOKEN not found in environment variables")
        print("Please ensure ASTRO_API_TOKEN is set in your .env file")
        return False

    print("✅ ASTRO_API_TOKEN found in environment")
    return True


def check_astro_cli():
    """Check if Astro CLI is installed and accessible."""
    try:
        result = subprocess.run(
            ["astro", "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✅ Astro CLI found: {version}")
            return True
        else:
            print(
                f"❌ Astro CLI not responding properly with error: {result.stderr}, exit code: {result.returncode}"
            )
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"❌ Astro CLI not found: {e}")
        print(
            "Please install Astro CLI: https://docs.astronomer.io/astro/cli/install-cli"
        )
        return False


def validate_astro_login():
    """Validate that Astro is properly logged in."""
    try:
        # Try a simple command that requires authentication
        result = subprocess.run(
            ["astro", "deployment", "list"], capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            print("✅ Astro login validation successful")
            print("You are properly authenticated with Astronomer")
            return True
        else:
            print("❌ Astro login validation failed")
            print(f"Error: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print("❌ Astro login validation timed out")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during validation: {e}")
        return False


def perform_astro_login():
    """Attempt to login to Astro using the token."""
    token = os.getenv("ASTRO_API_TOKEN")
    if not token:
        print("❌ Cannot login: ASTRO_API_TOKEN not available")
        return False

    try:
        print("🔄 Attempting to login to Astro...")
        result = subprocess.run(
            ["astro", "login", "--token-login", token],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            print("✅ Successfully logged into Astro")
            return True
        else:
            print("❌ Failed to login to Astro")
            print(f"Error: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print("❌ Astro login timed out")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during login: {e}")
        return False


def main():
    """Main function to validate Astro login."""
    print("🚀 Astro Login Validation Helper")
    print("=" * 40)

    # Load environment variables
    load_env_file()

    # Step 1: Check if token is available
    if not check_astro_token():
        sys.exit(1)

    # Step 2: Check if Astro CLI is installed
    if not check_astro_cli():
        sys.exit(1)

    # Step 3: Validate current login status
    if validate_astro_login():
        print("\n🎉 All checks passed! Astro is ready to use.")
        sys.exit(0)

    # Step 4: If validation failed, attempt login
    print("\n🔄 Login validation failed. Attempting to login...")
    if perform_astro_login():
        # Re-validate after login
        if validate_astro_login():
            print("\n🎉 Login successful! Astro is ready to use.")
            sys.exit(0)
        else:
            print("\n❌ Login appeared successful but validation still fails")
            sys.exit(1)
    else:
        print("\n❌ Failed to login to Astro")
        sys.exit(1)


if __name__ == "__main__":
    main()
