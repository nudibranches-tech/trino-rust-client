use log::error;
use log::info;
use log::warn;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

#[derive(Debug)]
pub struct DockerCompose {
    project_name: String,
    docker_compose_dir: String,
}

/// A utility to manage docker compose.
impl DockerCompose {
    pub fn new(project_name: impl ToString, docker_compose_dir: impl ToString) -> Self {
        Self {
            project_name: project_name.to_string(),
            docker_compose_dir: docker_compose_dir.to_string(),
        }
    }

    pub fn project_name(&self) -> &str {
        self.project_name.as_str()
    }

    /// Get the OS and architecture of the host.
    // This is used to set the DOCKER_DEFAULT_PLATFORM environment variable for the docker compose commands.
    fn get_os_arch() -> String {
        let mut cmd = Command::new("docker");
        cmd.arg("info")
            .arg("--format")
            .arg("{{.OSType}}/{{.Architecture}}");

        let output = cmd.output().expect("Failed to execute docker info");
        if output.status.success() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            // Below tries an alternative path if the above path fails
            let mut alt_cmd = Command::new("docker");
            alt_cmd
                .arg("info")
                .arg("--format")
                .arg("{{.Version.OsArch}}");
            let alt_output = alt_cmd.output().expect("Failed to execute docker info");
            String::from_utf8_lossy(&alt_output.stdout)
                .trim()
                .to_string()
        }
    }

    /// Start the docker compose services.
    pub fn up(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.env("DOCKER_DEFAULT_PLATFORM", Self::get_os_arch());

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "up",
            "-d",
        ]);

        info!(
            "Starting docker compose in {}, project name: {}",
            self.docker_compose_dir, self.project_name
        );
        let output = cmd.output().expect("Failed to execute docker compose up");

        if !output.status.success() {
            error!(
                "Docker compose up failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            panic!("Docker compose up failed!")
        }

        // Wait for the coordinator and worker to be healthy
        self.wait_for_service_healthy("minio", 120);
        self.wait_for_service_healthy("coordinator", 120);

        // Wait a bit more to ensure Trino is ready to accept queries.
        let extra_wait_time = 15;
        info!(
            "Waiting an extra {}s for Trino server initialization...",
            extra_wait_time
        );
        sleep(Duration::from_secs(extra_wait_time));
    }

    /// Wait for a service to be healthy.
    fn wait_for_service_healthy(&self, service_name: &str, timeout_seconds: u64) {
        let container_name = format!("{}-{}-1", self.project_name, service_name);
        let start = std::time::Instant::now();

        info!(
            "Waiting for {} to be healthy (timeout: {}s)",
            container_name, timeout_seconds
        );

        loop {
            let mut cmd = Command::new("docker");
            cmd.arg("inspect")
                .arg("--format")
                .arg("{{.State.Health.Status}}")
                .arg(&container_name);

            if let Ok(output) = cmd.output() {
                if output.status.success() {
                    let health_status = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if health_status == "healthy" {
                        info!("{} is healthy", container_name);
                        return;
                    }
                }
            }

            if start.elapsed().as_secs() > timeout_seconds {
                error!("Timeout waiting for {} to be healthy", container_name);
                panic!("Timeout waiting for {} to be healthy", container_name);
            }

            sleep(Duration::from_secs(2));
        }
    }

    /// Stop and remove all containers created by this docker compose.
    pub fn down(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "down",
            "-v",
            "--remove-orphans",
        ]);

        info!(
            "Stopping docker compose in {}, project name: {}",
            self.docker_compose_dir, self.project_name
        );
        let output = cmd.output().expect("Failed to execute docker compose down");

        if !output.status.success() {
            warn!(
                "Docker compose down had issues (this is usually fine): {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
