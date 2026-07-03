//! Bounded external-process execution helpers.

use anyhow::{Context, Result, bail};
use std::ffi::OsStr;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub async fn run<I, S>(program: &str, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("start {program}"))?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        bail!(
            "{program} exited with {}: {error}",
            output.status.code().unwrap_or(-1)
        );
    }
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.stderr.is_empty() {
        text.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    Ok(text)
}

pub async fn docker<I, S>(args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run("docker", args).await
}

pub async fn docker_with_env<I, S>(args: I, key: &str, value: &str) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("docker")
        .args(args)
        .env(key, value)
        .kill_on_drop(true)
        .output()
        .await
        .context("start docker")?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        bail!(
            "docker exited with {}: {error}",
            output.status.code().unwrap_or(-1)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub async fn docker_with_input<I, S>(args: I, input: &[u8]) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut child = Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("start docker")?;

    let mut stdin = child.stdin.take().context("open docker stdin")?;
    stdin.write_all(input).await.context("write docker stdin")?;
    drop(stdin);

    let output = child.wait_with_output().await.context("wait for docker")?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        bail!(
            "docker exited with {}: {error}",
            output.status.code().unwrap_or(-1)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
