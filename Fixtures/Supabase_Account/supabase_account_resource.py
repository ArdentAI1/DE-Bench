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
    isArdent = request.param.get("useArdent", False)

    # Create unique email for this test to avoid conflicts
    test_id = str(uuid.uuid4())[:8]
    unique_email = f"test-{test_id}@example.com"

    # Create user with shared admin client (stays in service role context)
    resp = supabase_client.auth.admin.create_user({
        "email": unique_email,
        "password": "Str0ngP@ss!",
        "email_confirm": True
    })

    response = {}

    # Store the user ID for cleanup
    user_id = resp.user.id

    response["userID"] = user_id


    # Manually craft JWT token - no session state changes!
    

    

    # Test the JWT token with your backend
    if isArdent:

        jwt_payload = {
        "sub": user_id,                    # User ID (subject)
        "email": unique_email,             # User email
        "role": "authenticated",           # User role
        "aud": "authenticated",            # Audience
        "iss": "supabase",                # Issuer
        "exp": int(time.time()) + 3600,   # Expires in 1 hour
        "iat": int(time.time()),          # Issued at now
        "session_id": str(uuid.uuid4())   # Unique session ID
    }
    
        jwt_token = jwt.encode(
            jwt_payload,
            os.environ["SUPABASE_JWT_SECRET"],
            algorithm="HS256"
        )
    
    # Create a fake refresh token (if needed - usually not used in tests)
        refresh_token = f"refresh_token_{uuid.uuid4()}"


        token_creation_response = requests.post(
            f"{os.getenv('ARDENT_BASE_URL')}/v1/api/createKeys",
            json={
                "userID": user_id  # Changed from "userId" to "userID"
            },
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json"
            },
            timeout=10
        )

        # Check if the response was successful before trying to parse JSON
        if not token_creation_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to create keys: HTTP {token_creation_response.status_code} - {token_creation_response.text}"
            )

        token_data = token_creation_response.json()
        publicKey = token_data["publicKey"]
        secretKey = token_data["secretKey"]


        response["publicKey"] = publicKey
        response["secretKey"] = secretKey

 




            
            


    yield response

    
    try:
        # Only delete keys if they were created
        if isArdent and 'publicKey' in response:
            delete_key_response = requests.delete(
                f"{os.getenv('ARDENT_BASE_URL')}/v1/api/deleteKey",
                json={
                    "userID": user_id,
                    "publicKey": response["publicKey"],  # Use from response dict
                },
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                    "Content-Type": "application/json"
                },
                timeout=10
            )
        
        # Always delete the user
        supabase_client.auth.admin.delete_user(user_id)
        
    except Exception as e:
        print(f"Error deleting user: {e}")


