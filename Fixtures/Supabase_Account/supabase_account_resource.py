from supabase import create_client
import os
import pytest
import requests
import jwt
import time
import uuid

from Configs.SupabaseConfig import supabase_client


@pytest.fixture(scope="function")
def supabase_account_resource(request):
    """
    Pytest fixture for Supabase account resource.

    If Supabase is configured (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY):
      - Creates dynamic test user and API key

    Otherwise:
      - Uses ARDENT_API_KEY from environment (required for Ardent mode)
    """
    mode = request.config.getoption("--mode")

    # Check if Supabase is configured
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    has_supabase = bool(supabase_url and supabase_service_key)

    # If Supabase is NOT configured, use environment ARDENT_API_KEY
    if not has_supabase:
        print(
            "⚠️  Supabase not configured (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY missing)"
        )

        if mode == "Ardent":
            ardent_api_key = os.getenv("ARDENT_API_KEY")
            if not ardent_api_key:
                raise ValueError(
                    "❌ ARDENT_API_KEY environment variable is required when Supabase is not configured.\n"
                    "   Please set ARDENT_API_KEY in your .env file.\n"
                    "   You can generate an API key from your Ardent instance dashboard:\n"
                    "   1. Go to your Ardent instance\n"
                    "   2. Navigate to Settings > API Keys\n"
                    "   3. Create a new API key with required scopes\n"
                    "   4. Copy the key (shown only once!) and add to .env:\n"
                    "      ARDENT_API_KEY=sk-ard_test_xxxxx"
                )

            print(f"✅ Using ARDENT_API_KEY from environment")
            response = {
                "userID": "env-user",  # Placeholder
                "api_key": ardent_api_key,
            }
            yield response
            # No cleanup needed for environment API key
            return
        else:
            # Non-Ardent modes don't need API keys
            response = {"userID": "non-ardent-user"}
            yield response
            return

    # Supabase IS configured - create dynamic test user and API key
    print("✅ Supabase configured - creating dynamic test user and API key")

    # Create unique email for this test to avoid conflicts
    test_id = str(uuid.uuid4())[:8]
    unique_email = f"test-{test_id}@example.com"

    # Create user with shared admin client (stays in service role context)
    resp = supabase_client.auth.admin.create_user(
        {"email": unique_email, "password": "Str0ngP@ss!", "email_confirm": True}
    )

    response = {}

    # Store the user ID for cleanup
    user_id = resp.user.id

    response["userID"] = user_id

    # Manually craft JWT token - no session state changes!

    # Test the JWT token with your backend
    if mode == "Ardent":
        jwt_payload = {
            "sub": user_id,  # User ID (subject)
            "email": unique_email,  # User email
            "role": "authenticated",  # User role
            "aud": "authenticated",  # Audience
            "iss": "supabase",  # Issuer
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at now
            "session_id": str(uuid.uuid4()),  # Unique session ID
        }

        jwt_token = jwt.encode(
            jwt_payload, os.environ["SUPABASE_JWT_SECRET"], algorithm="HS256"
        )

        # Create a fake refresh token (if needed - usually not used in tests)
        refresh_token = f"refresh_token_{uuid.uuid4()}"

        # Get org_id using V2 API
        my_orgs_response = requests.get(
            f"{os.getenv('ARDENT_BASE_URL')}/v1/my-orgs",
            headers={
                "Authorization": f"Bearer {jwt_token}",
            },
            timeout=10,
        )

        if not my_orgs_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to get orgs: HTTP {my_orgs_response.status_code} - {my_orgs_response.text}"
            )

        orgs_data = my_orgs_response.json()

        # If no orgs exist, create one for the new user
        if not orgs_data.get("orgs") or len(orgs_data["orgs"]) == 0:
            create_org_response = requests.post(
                f"{os.getenv('ARDENT_BASE_URL')}/v1/orgs",
                json={"name": f"DE-Bench Test Org {test_id}"},
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )

            if not create_org_response.ok:
                raise requests.exceptions.ConnectionError(
                    f"Failed to create org: HTTP {create_org_response.status_code} - {create_org_response.text}"
                )

            org_data = create_org_response.json()
            org_id = org_data["id"]
        else:
            org_id = orgs_data["orgs"][0]["org_id"]

        # Create API key using V2 API
        token_creation_response = requests.post(
            f"{os.getenv('ARDENT_BASE_URL')}/v1/orgs/{org_id}/api-keys",
            json={
                "name": f"DE-Bench Test Key {test_id}",
                "role_id": "role_org_owner",  # Use org owner role
                "scopes": [],  # Use all role permissions
            },
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )

        # Check if the response was successful before trying to parse JSON
        if not token_creation_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to create keys: HTTP {token_creation_response.status_code} - {token_creation_response.text}"
            )

        token_data = token_creation_response.json()
        api_key = token_data["api_key"]  # V2 returns single api_key (bearer token)

        response["api_key"] = api_key
        response["api_key_id"] = token_data["api_key_id"]  # Store for cleanup
        response["org_id"] = org_id  # Store org_id for reference

    yield response

    try:
        # Only delete keys if they were created (V2 API)
        if mode == "Ardent" and "api_key_id" in response and "org_id" in response:
            delete_key_response = requests.delete(
                f"{os.getenv('ARDENT_BASE_URL')}/v1/orgs/{response['org_id']}/api-keys/{response['api_key_id']}",
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                },
                timeout=10,
            )

        # Always delete the user
        supabase_client.auth.admin.delete_user(user_id)

    except Exception as e:
        print(f"Error deleting user: {e}")
