mod docker;

use docker::DockerCompose;
use std::sync::Once;

pub struct TestFixture {
    pub _docker_compose: DockerCompose,
    pub coordinator_host: String,
    pub coordinator_port: u16,
}

static INIT: Once = Once::new();

// Set up the test fixture.
pub fn set_up() -> DockerCompose {
    let docker_compose = DockerCompose::new(
        "trino_integration_tests",
        format!("{}/test_setup", env!("CARGO_MANIFEST_DIR")),
    );

    // Ensure docker compose is started only once
    INIT.call_once(|| {
        // Clean up any existing containers from previous runs
        docker_compose.down();
        // Start the containers
        docker_compose.up();
    });

    docker_compose
}

// Set up the test fixture.
pub fn set_test_fixture(_func: &str) -> TestFixture {
    let docker_compose = set_up();

    let coordinator_port = 8080;
    let coordinator_host = "localhost".to_string();

    TestFixture {
        _docker_compose: docker_compose,
        coordinator_host,
        coordinator_port,
    }
}
