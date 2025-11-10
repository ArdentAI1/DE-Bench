from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Generic, Optional, List
from typing_extensions import TypedDict
from braintrust import traced

# Generic type variables for configuration and resource data
ConfigT = TypeVar("ConfigT", bound=Dict[str, Any])
ResourceT = TypeVar("ResourceT", bound=Dict[str, Any])
SessionT = TypeVar("SessionT", bound=Dict[str, Any])


class DEBenchFixture(ABC, Generic[ConfigT, ResourceT, SessionT]):
    """
    Abstract base class for all DE-Bench fixtures.

    This ensures a consistent interface across all resource types:
    - test_setup: Creates and initializes the resource
    - test_teardown: Cleans up and destroys the resource
    - get_resource_type: Returns a string identifier for the resource type

    Fixtures can optionally be initialized with custom configuration.

    Session-level support:
    - session_setup: Creates shared session resources (e.g., Airflow server)
    - session_teardown: Cleans up shared session resources
    - requires_session_setup: Indicates if this fixture needs session-level setup
    """

    def __init__(
        self,
        custom_config: Optional[ConfigT] = None,
        session_data: Optional[SessionT] = None,
    ):
        """
        Initialize the fixture with optional custom configuration and session data.

        Args:
            custom_config: Optional configuration to use instead of default config.
                          If provided, this will be used by test_setup().
            session_data: Optional session data from session_setup(), passed down from runner.
        """
        self.custom_config = custom_config
        self.session_data = session_data

    @abstractmethod
    def test_setup(self, resource_config: Optional[ConfigT] = None) -> ResourceT:
        """
        Set up and initialize the resource based on configuration.

        Args:
            resource_config: Configuration dictionary specific to this resource type.
                           If None, will use custom_config from __init__ or get_default_config().

        Returns:
            Resource data dictionary containing metadata and connection info
        """
        pass

    def _test_setup(self, resource_config: Optional[ConfigT] = None) -> ResourceT:
        """
        Set up and initialize the resource based on configuration.
        """

        @traced(name=f"{self.get_resource_type()}.test_setup")
        def inner_test_setup(
            resource_config: Optional[ConfigT] = None,
        ) -> ResourceT:
            """
            Inner setup resource method.
            """
            return self.test_setup(resource_config)

        resource_data = inner_test_setup(resource_config)
        self._resource_data = resource_data
        return resource_data

    @abstractmethod
    def test_teardown(self, resource_data: ResourceT) -> None:
        """
        Clean up and destroy the resource.

        Args:
            resource_data: Resource data returned from test_setup
        """
        pass

    def _test_teardown(self) -> None:
        """
        Clean up and destroy the resource.
        """

        @traced(name=f"{self.get_resource_type()}.test_teardown")
        def inner_test_teardown(resource_data: ResourceT) -> None:
            """
            Inner teardown resource method.
            """
            return self.test_teardown(resource_data)

        inner_test_teardown(self._resource_data)

    @classmethod
    @abstractmethod
    def get_resource_type(cls) -> str:
        """
        Return the resource type identifier (e.g., 'mongo_resource', 'postgres_resource').

        Returns:
            String identifier for this resource type
        """
        pass

    @classmethod
    @abstractmethod
    def get_default_config(cls) -> ConfigT:
        """
        Return a default configuration for this resource type.
        Useful for tests that need minimal setup.

        Returns:
            Default configuration dictionary
        """
        pass

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create a config section for this fixture using its resource data.
        This method should return the config section that would be used by the AI agent.

        Returns:
            Dictionary containing the config section for this resource type
        """
        # Default implementation returns empty config
        # Subclasses should override this to provide actual config creation
        return {}

    # ========== Session-Level Methods (Optional) ==========

    @classmethod
    def requires_session_setup(cls) -> bool:
        """
        Indicate whether this fixture requires session-level setup/teardown.

        Session-level fixtures are set up once before all tests and torn down
        after all tests complete. Useful for expensive resources like Airflow
        servers that should be shared across multiple tests.

        Returns:
            True if this fixture needs session-level setup, False otherwise
        """
        return False

    def session_setup(
        self, session_configs: Optional[List[ConfigT]] = None
    ) -> Optional[SessionT]:
        """
        Set up session-level resources that will be shared across multiple tests.

        This method is called once before any tests run, for fixtures that
        return True from requires_session_setup().

        It receives ALL configs from ALL tests to allow intelligent decisions
        about what infrastructure to provision (e.g., if tests need AKS + ECS,
        both can be set up in a single session_setup call).

        Args:
            session_configs: List of ALL configurations from ALL tests in the session. If None or empty, uses
                          custom_config from __init__ or get_default_config().

        Returns:
            Session data dictionary that will be passed to individual fixtures
        """
        return None

    def session_teardown(self, session_data: Optional[SessionT] = None) -> None:
        """
        Clean up session-level resources.

        This method is called once after all tests complete, for fixtures that
        return True from requires_session_setup().

        Args:
            session_data: Session data returned from session_setup()
        """
        pass
