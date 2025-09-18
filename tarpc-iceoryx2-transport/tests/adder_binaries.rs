use std::{
    error::Error,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

struct ChildGuard {
    child: Option<Child>,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn client_and_server_binaries_add_numbers() -> Result<(), Box<dyn Error>> {
    let server_bin = env!("CARGO_BIN_EXE_adder_server");
    let client_bin = env!("CARGO_BIN_EXE_adder_client");
    let service_name = format!(
        "tarpc/test/bin/{}/{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );

    let server_child = Command::new(server_bin)
        .arg(&service_name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut server = ChildGuard {
        child: Some(server_child),
    };

    thread::sleep(Duration::from_millis(500));

    let output = Command::new(client_bin)
        .arg(&service_name)
        .arg("20")
        .arg("22")
        .output()?;
    assert!(
        output.status.success(),
        "client exited with failure: {:?}",
        output
    );

    let stdout = String::from_utf8(output.stdout)?;
    assert_eq!(stdout.trim(), "42");

    if let Some(mut child) = server.child.take() {
        let _ = child.kill();
        let _ = child.wait();
    }

    Ok(())
}
