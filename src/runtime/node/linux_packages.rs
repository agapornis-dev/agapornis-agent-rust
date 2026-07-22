//! Fixed-command Linux package updates. No request data reaches a shell.

use anyhow::{Context, Result, bail};
use std::{path::Path, sync::Arc};
use tokio::{fs, process::Command, sync::Mutex};

const APT: &str = "/usr/bin/apt-get";
const PACMAN: &str = "/usr/bin/pacman";
const CHECKUPDATES: &str = "/usr/bin/checkupdates";
const DNF: &str = "/usr/bin/dnf";
const APK: &str = "/sbin/apk";
const SUDO: &str = "/usr/bin/sudo";
const MAX_PACKAGES: usize = 2_000;

#[derive(Clone, Default)]
pub struct LinuxPackageUpdater(Arc<Mutex<()>>);

pub struct LinuxUpdateResult {
    pub message: String,
    pub packages: Vec<PackageUpgrade>,
    pub reboot_required: bool,
    pub distribution: String,
    pub manager: &'static str,
    pub preview_command: &'static str,
    pub apply_command: &'static str,
}

pub struct PackageUpgrade {
    pub name: String,
    pub current: String,
    pub candidate: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Manager {
    Apt,
    Pacman,
    Dnf,
    Apk,
}

struct Platform {
    distribution: String,
    manager: Manager,
}

impl LinuxPackageUpdater {
    pub async fn preview(&self) -> Result<LinuxUpdateResult> {
        ensure_linux()?;
        let _guard = self.0.lock().await;
        let platform = platform().await?;
        let packages = platform.manager.preview().await?;
        Ok(result(platform, packages, false))
    }

    pub async fn apply(&self) -> Result<LinuxUpdateResult> {
        ensure_linux()?;
        let _guard = self.0.lock().await;
        let platform = platform().await?;
        let packages = platform.manager.preview().await?;
        if !packages.is_empty() {
            platform.manager.apply().await?;
        }
        Ok(result(platform, packages, true))
    }
}

impl Manager {
    async fn preview(self) -> Result<Vec<PackageUpgrade>> {
        match self {
            Self::Apt => {
                run(APT, &["update"], &[0], true).await?;
                Ok(parse_apt(
                    &run(APT, &["--simulate", "upgrade"], &[0], true).await?,
                ))
            }
            Self::Pacman => {
                if !Path::new(CHECKUPDATES).is_file() {
                    bail!("Arch previews require pacman-contrib (missing /usr/bin/checkupdates)")
                }
                Ok(parse_pacman(
                    &run(CHECKUPDATES, &["--nocolor"], &[0, 2], false).await?,
                ))
            }
            Self::Dnf => Ok(parse_dnf(
                &run(DNF, &["-q", "--refresh", "check-upgrade"], &[0, 100], true).await?,
            )),
            Self::Apk => {
                run(APK, &["update"], &[0], true).await?;
                Ok(parse_apk(
                    &run(APK, &["--simulate", "upgrade"], &[0], true).await?,
                ))
            }
        }
    }

    async fn apply(self) -> Result<()> {
        let (program, args): (&str, &[&str]) = match self {
            Self::Apt => (APT, &["-y", "upgrade"]),
            Self::Pacman => (PACMAN, &["-Syu", "--noconfirm", "--noprogressbar"]),
            Self::Dnf => (DNF, &["-y", "--refresh", "upgrade"]),
            Self::Apk => (APK, &["upgrade"]),
        };
        run(program, args, &[0], true).await.map(|_| ())
    }

    fn name(self) -> &'static str {
        match self {
            Self::Apt => "apt",
            Self::Pacman => "pacman",
            Self::Dnf => "dnf",
            Self::Apk => "apk",
        }
    }
    fn preview_command(self) -> &'static str {
        match self {
            Self::Apt => "sudo /usr/bin/apt-get update && sudo /usr/bin/apt-get --simulate upgrade",
            Self::Pacman => "/usr/bin/checkupdates --nocolor",
            Self::Dnf => "sudo /usr/bin/dnf -q --refresh check-upgrade",
            Self::Apk => "sudo /sbin/apk update && sudo /sbin/apk --simulate upgrade",
        }
    }
    fn apply_command(self) -> &'static str {
        match self {
            Self::Apt => "sudo /usr/bin/apt-get update && sudo /usr/bin/apt-get -y upgrade",
            Self::Pacman => "sudo /usr/bin/pacman -Syu --noconfirm --noprogressbar",
            Self::Dnf => "sudo /usr/bin/dnf -y --refresh upgrade",
            Self::Apk => "sudo /sbin/apk update && sudo /sbin/apk upgrade",
        }
    }
}

fn result(platform: Platform, packages: Vec<PackageUpgrade>, applied: bool) -> LinuxUpdateResult {
    let count = packages.len();
    let manager = platform.manager;
    LinuxUpdateResult {
        message: if applied {
            format!("Updated {count} packages with {}.", manager.name())
        } else {
            format!("{count} packages can be updated with {}.", manager.name())
        },
        reboot_required: applied
            && (Path::new("/var/run/reboot-required").exists()
                || packages.iter().any(reboots_host)),
        packages,
        distribution: platform.distribution,
        manager: manager.name(),
        preview_command: manager.preview_command(),
        apply_command: manager.apply_command(),
    }
}

async fn platform() -> Result<Platform> {
    let release = fs::read_to_string("/etc/os-release")
        .await
        .context("read /etc/os-release")?;
    let id = release_value(&release, "ID")
        .unwrap_or_default()
        .to_lowercase();
    let like = release_value(&release, "ID_LIKE")
        .unwrap_or_default()
        .to_lowercase();
    let distribution = release_value(&release, "PRETTY_NAME")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| id.clone())
        .chars()
        .filter(|value| !value.is_control())
        .take(120)
        .collect();
    let manager = manager_for_release(&id, &like)
        .or_else(manager_from_installed_binary)
        .context("unsupported Linux distribution; expected apt-get, pacman, dnf, or apk")?;
    Ok(Platform {
        distribution,
        manager,
    })
}

fn manager_for_release(id: &str, like: &str) -> Option<Manager> {
    let matches = |names: &[&str]| {
        id.split_whitespace()
            .chain(like.split_whitespace())
            .any(|value| names.contains(&value))
    };
    if matches(&["alpine"]) {
        Some(Manager::Apk)
    } else if matches(&["arch", "manjaro"]) {
        Some(Manager::Pacman)
    } else if matches(&["debian", "ubuntu"]) {
        Some(Manager::Apt)
    } else if matches(&["fedora", "rhel", "centos"]) {
        Some(Manager::Dnf)
    } else {
        None
    }
}

fn manager_from_installed_binary() -> Option<Manager> {
    [
        (APT, Manager::Apt),
        (PACMAN, Manager::Pacman),
        (DNF, Manager::Dnf),
        (APK, Manager::Apk),
    ]
    .into_iter()
    .find_map(|(path, manager)| Path::new(path).is_file().then_some(manager))
}

async fn run(program: &str, args: &[&str], success: &[i32], root: bool) -> Result<String> {
    let mut command = if root && needs_sudo() {
        let mut command = Command::new(SUDO);
        command.args(["-n", program]);
        command
    } else {
        Command::new(program)
    };
    let output = command
        .args(args)
        .env("LC_ALL", "C")
        .env("DEBIAN_FRONTEND", "noninteractive")
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| {
            format!(
                "start fixed {} command",
                Path::new(program)
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
            )
        })?;
    let code = output.status.code().unwrap_or(-1);
    if !success.contains(&code) {
        let detail = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} failed: {}",
            Path::new(program)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy(),
            detail.trim().chars().take(4_096).collect::<String>()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn parse_apt(output: &str) -> Vec<PackageUpgrade> {
    output
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("Inst ")?;
            Some(PackageUpgrade {
                name: rest.split_whitespace().next()?.to_owned(),
                current: between(rest, '[', ']').unwrap_or_default(),
                candidate: rest
                    .split_once('(')?
                    .1
                    .split_whitespace()
                    .next()?
                    .to_owned(),
            })
        })
        .take(MAX_PACKAGES)
        .collect()
}

fn parse_pacman(output: &str) -> Vec<PackageUpgrade> {
    output
        .lines()
        .filter_map(|line| {
            let mut values = line.split_whitespace();
            Some(PackageUpgrade {
                name: values.next()?.to_owned(),
                current: values.next()?.to_owned(),
                candidate: values.nth(1)?.to_owned(),
            })
        })
        .take(MAX_PACKAGES)
        .collect()
}

fn parse_dnf(output: &str) -> Vec<PackageUpgrade> {
    output
        .lines()
        .filter_map(|line| {
            let mut values = line.split_whitespace();
            let name = values.next()?;
            let candidate = values.next()?;
            values.next()?;
            if !name.contains('.') || !candidate.chars().any(|value| value.is_ascii_digit()) {
                return None;
            }
            Some(PackageUpgrade {
                name: name.to_owned(),
                current: String::new(),
                candidate: candidate.to_owned(),
            })
        })
        .take(MAX_PACKAGES)
        .collect()
}

fn parse_apk(output: &str) -> Vec<PackageUpgrade> {
    output
        .lines()
        .filter_map(|line| {
            let rest = line
                .split_once(" Upgrading ")
                .or_else(|| line.split_once(" Installing "))?
                .1;
            let (name, versions) = rest.split_once(" (")?;
            let versions = versions.trim_end_matches(')');
            let (current, candidate) = versions
                .split_once(" -> ")
                .map(|(old, new)| (old, new))
                .unwrap_or(("", versions));
            Some(PackageUpgrade {
                name: name.to_owned(),
                current: current.to_owned(),
                candidate: candidate.to_owned(),
            })
        })
        .take(MAX_PACKAGES)
        .collect()
}

fn release_value(text: &str, key: &str) -> Option<String> {
    let value = text.lines().find_map(|line| {
        line.split_once('=')
            .filter(|(name, _)| *name == key)
            .map(|(_, value)| value.trim())
    })?;
    Some(
        value
            .trim_matches(['\'', '"'])
            .replace("\\\"", "\"")
            .replace("\\\\", "\\"),
    )
}

fn between(value: &str, start: char, end: char) -> Option<String> {
    Some(value.split_once(start)?.1.split_once(end)?.0.to_owned())
}
fn reboots_host(package: &PackageUpgrade) -> bool {
    package.name == "linux"
        || package.name.starts_with("linux-")
        || package.name == "kernel"
        || package.name.starts_with("kernel.")
}

#[cfg(target_family = "unix")]
fn needs_sudo() -> bool {
    unsafe { libc::geteuid() != 0 }
}
#[cfg(not(target_family = "unix"))]
fn needs_sudo() -> bool {
    true
}
fn ensure_linux() -> Result<()> {
    if cfg!(target_os = "linux") {
        Ok(())
    } else {
        bail!("Linux package updates are supported only on Linux nodes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_supported_distribution_families() {
        assert_eq!(manager_for_release("ubuntu", "debian"), Some(Manager::Apt));
        assert_eq!(
            manager_for_release("endeavouros", "arch"),
            Some(Manager::Pacman)
        );
        assert_eq!(
            manager_for_release("rocky", "rhel centos fedora"),
            Some(Manager::Dnf)
        );
        assert_eq!(manager_for_release("alpine", ""), Some(Manager::Apk));
        assert_eq!(manager_for_release("gentoo", ""), None);
    }

    #[test]
    fn parses_package_manager_previews() {
        let apt = parse_apt("Inst bash [5.2-1] (5.2-2 Debian [amd64])");
        let pacman = parse_pacman("bash 5.2-1 -> 5.2-2");
        let dnf = parse_dnf("bash.x86_64 5.2-2 updates");
        let apk = parse_apk("(1/1) Upgrading bash (5.2-r0 -> 5.2-r1)");
        for packages in [&apt, &pacman, &dnf, &apk] {
            assert_eq!(packages.len(), 1);
        }
        assert_eq!(
            (&apt[0].current, &apt[0].candidate),
            (&"5.2-1".into(), &"5.2-2".into())
        );
        assert_eq!(
            (&pacman[0].current, &pacman[0].candidate),
            (&"5.2-1".into(), &"5.2-2".into())
        );
        assert_eq!(dnf[0].candidate, "5.2-2");
        assert_eq!(
            (&apk[0].current, &apk[0].candidate),
            (&"5.2-r0".into(), &"5.2-r1".into())
        );
    }

    #[test]
    fn commands_are_fixed_for_every_adapter() {
        for manager in [Manager::Apt, Manager::Pacman, Manager::Dnf, Manager::Apk] {
            assert!(!manager.preview_command().is_empty());
            assert!(!manager.apply_command().is_empty());
        }
    }
}
