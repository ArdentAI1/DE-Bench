import pytest
import json
import time
import os
import subprocess
import uuid
from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict
from Configs.MongoConfig import syncMongoClient
from pymongo.errors import CollectionInvalid
from Fixtures.base_fixture import DEBenchFixture


# Type definitions for MongoDB resources (matching Snowflake pattern)
class MongoS3Config(TypedDict, total=False):
    bucket_url: str
    s3_key: str
    aws_key_id: str
    aws_secret_key: str


class MongoCollectionConfig(TypedDict):
    name: str
    data: List[Dict[str, Any]]


class MongoDatabaseConfig(TypedDict):
    name: str
    collections: List[MongoCollectionConfig]


class MongoResourceConfig(TypedDict):
    resource_id: str
    database: Optional[str]  # Only used for inline data (not needed for S3 - DB name comes from BSON dump)
    s3_config: Optional[MongoS3Config]  # S3 config for BSON dumps
    databases: Optional[List[MongoDatabaseConfig]]  # Legacy support


class MongoResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    database: str  # Like Snowflake
    created_resources: List[Dict[str, str]]


class MongoDBFixture(
    DEBenchFixture[MongoResourceConfig, MongoResourceData, Dict[str, Any]]
):
    """
    MongoDB fixture implementation using DEBenchFixture pattern.

    Features:
    - Database creation with unique naming
    - S3 integration for BSON data loading (like Snowflake)
    - Inline data support for simple tests
    - Automatic cleanup of created resources
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """MongoDB doesn't require session-level setup"""
        return False

    def session_setup(
        self, session_config: Optional[MongoResourceConfig] = None
    ) -> Dict[str, Any]:
        """No session setup needed for MongoDB"""
        return {}

    def session_teardown(self, session_data: Optional[Dict[str, Any]] = None) -> None:
        """No session teardown needed for MongoDB"""
        pass

    def get_client(self):
        """Get the MongoDB client. Useful for validation and testing."""
        return syncMongoClient

    def get_database(self, database_name: str):
        """Get a specific MongoDB database. Useful for validation and testing."""
        return syncMongoClient[database_name]

    def test_setup(
        self, resource_config: Optional[MongoResourceConfig] = None
    ) -> MongoResourceData:
        """Set up MongoDB resource - supports inline data or S3 restore"""
        # Determine which config to use (matching Snowflake pattern)
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config.get("resource_id", f"mongo_resource_{int(time.time())}")
        print(f"🍃 Setting up MongoDB resource: {resource_id}")

        # Store connection string for later use
        self._connection_string = os.getenv("MONGODB_URI")

        creation_start = time.time()
        created_resources = []

        try:
            # NEW: Handle S3 config
            # Note: For S3/BSON, database name comes from the dump, not config
            if s3_config := config.get("s3_config"):
                return self._restore_from_s3(
                    config, resource_id, creation_start
                )

            # EXISTING: Handle inline data or legacy databases array
            # For non-S3 configs, generate database name
            timestamp = int(time.time())
            test_uuid = uuid.uuid4().hex[:8]
            database_name = config.get("database") or f"BENCH_DB_{timestamp}_{test_uuid}"
            
            if "databases" in config:
                # Legacy pattern support
                for db_config in config["databases"]:
                    db_name = db_config["name"]
                    db = syncMongoClient[db_name]

                    if "collections" in db_config:
                        for collection_config in db_config["collections"]:
                            collection_name = collection_config["name"]

                            # Create collection with error handling
                            try:
                                db.create_collection(collection_name)
                            except CollectionInvalid:
                                db.drop_collection(collection_name)
                                db.create_collection(collection_name)

                            created_resources.append(
                                {"db": db_name, "collection": collection_name}
                            )

                            # Add data if specified
                            if "data" in collection_config and collection_config["data"]:
                                collection = db[collection_name]
                                data = collection_config["data"]

                                # Use insert_many for better performance
                                if len(data) > 100:
                                    print(f"   Bulk inserting {len(data)} documents...")
                                    collection.insert_many(data, ordered=False)
                                else:
                                    for record in data:
                                        collection.insert_one(record)

                # Use first database name as primary
                database_name = config["databases"][0]["name"] if config["databases"] else database_name

        except Exception as e:
            print(f"❌ Failed to create MongoDB resource {resource_id}: {e}")
            # Clean up on failure
            self._cleanup_databases(created_resources)
            raise

        creation_end = time.time()
        creation_duration = creation_end - creation_start

        print(f"MongoDB resource creation took {creation_duration:.2f}s")

        resource_data = MongoResourceData(
            resource_id=resource_id,
            type="mongodb_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"MongoDB resource for {resource_id}",
            status="active",
            database=database_name,
            created_resources=created_resources,
        )

        # Store for later access during validation
        self._resource_data = resource_data

        print(f"✅ MongoDB resource {resource_id} ready! ({creation_duration:.2f}s)")
        return resource_data

    def _restore_from_s3(
        self,
        config: MongoResourceConfig,
        resource_id: str,
        creation_start: float,
    ) -> MongoResourceData:
        """Restore MongoDB from S3 (following Snowflake pattern)"""
        s3_config = config["s3_config"]

        # Resolve environment variable references (matching Snowflake lines 261-267)
        aws_key_id = s3_config.get("aws_key_id", "")
        if aws_key_id.startswith("env:"):
            aws_key_id = os.getenv(aws_key_id[4:])

        aws_secret_key = s3_config.get("aws_secret_key", "")
        if aws_secret_key.startswith("env:"):
            aws_secret_key = os.getenv(aws_secret_key[4:])

        bucket_url = s3_config.get("bucket_url", "")
        s3_key = s3_config.get("s3_key", "")

        # Parse bucket URL
        bucket = bucket_url.replace("s3://", "").rstrip("/")
        s3_path = f"s3://{bucket}/{s3_key}"

        print(f"📦 Restoring MongoDB from {s3_path}")

        # Set AWS credentials in environment for aws cli
        env = os.environ.copy()
        if aws_key_id:
            env["AWS_ACCESS_KEY_ID"] = aws_key_id
        if aws_secret_key:
            env["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

        # Determine format and use appropriate MongoDB tool
        if s3_key.endswith(".bson") or s3_key.endswith(".bson.gz"):
            # Use mongorestore for BSON dumps with namespace mapping for isolation
            # BSON dumps contain the original database name, we map it to a unique name
            
            # Extract base name from s3_key for clarity
            base_name = s3_key.split('/')[-1].replace('.bson.gz', '').replace('.bson', '')
            
            # Source database name (from the dump - always temp_seed_*)
            # This is what's IN the BSON file
            source_db = f"temp_seed_{base_name}"
            
            # Target database name (unique per test run)
            # MongoDB Atlas limit: 38 bytes for database names
            # Use short prefix + test name + uuid
            test_uuid = uuid.uuid4().hex[:8]
            
            # Shorten: test_{name}_{uuid}
            # If name is too long, truncate it
            max_name_len = 38 - 6 - 9  # 38 total - "test_" - "_{uuid}"
            short_name = base_name if len(base_name) <= max_name_len else base_name[:max_name_len]
            target_db = f"test_{short_name}_{test_uuid}"
            
            print(f"   Mapping: {source_db} → {target_db}")
            
            # Use mongorestore with proper namespace mapping
            cmd = f"""
                aws s3 cp {s3_path} - | \
                mongorestore --uri="{self._connection_string}" \
                    --nsFrom='{source_db}.*' \
                    --nsTo='{target_db}.*' \
                    --archive \
                    --gzip \
                    --stopOnError
            """
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)

            if result.returncode != 0:
                raise Exception(f"S3 restore failed: {result.stderr}")

            # Use the target database name
            database_name = target_db
            
            print(f"✅ Restored to database: {database_name}")

            # Track created resources
            db = syncMongoClient[database_name]
            collection_names = db.list_collection_names()
            created_resources = [
                {"db": database_name, "collection": coll} for coll in collection_names
            ]
            
        else:
            # Use mongoimport for JSON/NDJSON
            cmd = f"""
                aws s3 cp {s3_path} - | \
                mongoimport --uri="{self._connection_string}" \
                    --db={database_name} \
                    --collection=data \
                    --type=json
            """
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)

            if result.returncode != 0:
                raise Exception(f"S3 restore failed: {result.stderr}")

            print(f"✅ Restored to database: {database_name}")

            # Track created resources
            db = syncMongoClient[database_name]
            collection_names = db.list_collection_names()
            created_resources = [
                {"db": database_name, "collection": coll} for coll in collection_names
            ]

        creation_end = time.time()
        creation_duration = creation_end - creation_start

        return MongoResourceData(
            resource_id=resource_id,
            type="mongodb_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"MongoDB resource restored from S3",
            status="active",
            database=database_name,
            created_resources=created_resources,
        )

    def _cleanup_databases(self, created_resources: List[Dict[str, str]]) -> None:
        """Clean up created databases"""
        databases_to_drop = set()
        for resource in created_resources:
            databases_to_drop.add(resource["db"])

        for db_name in databases_to_drop:
            print(f"🗑️ Dropping database {db_name}")
            syncMongoClient.drop_database(db_name)

    def test_teardown(self, resource_data: MongoResourceData) -> None:
        """Clean up MongoDB resource - drops entire database (matching Snowflake)"""
        resource_id = resource_data.get("resource_id", "unknown")
        database_name = resource_data.get("database")

        print(f"🧹 Cleaning up MongoDB resource: {resource_id}")

        try:
            if database_name:
                print(f"🗑️ Dropping database {database_name}")
                syncMongoClient.drop_database(database_name)

            print(f"✅ MongoDB resource {resource_id} cleaned up successfully")

        except Exception as e:
            print(f"❌ Error cleaning up MongoDB resource {resource_id}: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "mongo_resource"

    @classmethod
    def get_default_config(cls) -> MongoResourceConfig:
        """Return default configuration for MongoDB resources"""
        timestamp = int(time.time())
        test_uuid = uuid.uuid4().hex[:8]

        return MongoResourceConfig(
            resource_id=f"mongodb_test_{timestamp}_{test_uuid}",
            database=None,  # Auto-generated for inline data; derived from BSON dump for S3
            s3_config=None,  # Optional: S3 config for BSON dumps
            databases=None,  # Optional: Legacy inline data
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create MongoDB config section using the fixture's resource data.

        Returns:
            Dictionary containing the mongoDB service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "MongoDB resource data not available - ensure test_setup was called"
            )

        # Extract database and collection information from created resources
        databases = {}
        for resource in resource_data["created_resources"]:
            db_name = resource["db"]
            collection_name = resource["collection"]

            if db_name not in databases:
                databases[db_name] = {"name": db_name, "collections": []}

            databases[db_name]["collections"].append({"name": collection_name})

        return {
            "mongodb": {
                "connection_string": self._connection_string,
                "database": resource_data.get("database"),  # Like Snowflake
                "databases": list(databases.values()),
                "created_resources": resource_data.get("created_resources", []),
            }
        }


# Global fixture instance - this is the only way to access MongoDB resources now
mongo_fixture = MongoDBFixture()


@pytest.fixture(scope="function")
def mongo_resource(request):
    """
    A function-scoped fixture that creates MongoDB resources based on template.
    Uses the DEBenchFixture implementation.
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting mongo_resource for {test_name}")

    build_template = request.param

    # Use the fixture class
    resource_data = mongo_fixture.test_setup(build_template)

    # Add test-specific metadata
    resource_data.update(
        {
            "test_name": test_name,
            "worker_pid": os.getpid(),
        }
    )

    fixture_end_time = time.time()
    print(
        f"Worker {os.getpid()}: MongoDB fixture setup took {fixture_end_time - start_time:.2f}s total"
    )

    yield resource_data

    # Use the fixture class for teardown
    mongo_fixture.test_teardown(resource_data)
