use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Both,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
            Self::Both => "both",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Protocol {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            "both" => Ok(Self::Both),
            _ => Err(ModelError::InvalidProtocol(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Active,
    RejectingNoHost,
    DegradedBindFailed,
    DegradedApplyFailed,
    DegradedCloseFailed,
}

impl RuntimeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::RejectingNoHost => "rejecting_no_host",
            Self::DegradedBindFailed => "degraded_bind_failed",
            Self::DegradedApplyFailed => "degraded_apply_failed",
            Self::DegradedCloseFailed => "degraded_close_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    BindFailed,
    ApplyFailed,
    CloseFailed,
    RestoreFailed,
    InvalidInput,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BindFailed => "bind_failed",
            Self::ApplyFailed => "apply_failed",
            Self::CloseFailed => "close_failed",
            Self::RestoreFailed => "restore_failed",
            Self::InvalidInput => "invalid_input",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ModelError {
    #[error("invalid protocol: {0}")]
    InvalidProtocol(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub target_port: Option<u16>,
    pub host: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub allocation_id: String,
    pub target_port: u16,
    pub host: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AllocationResource {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindingView {
    pub allocation_id: String,
    pub host: Option<String>,
    pub target_port: u16,
    pub effective_target_port: Option<u16>,
    pub effective_host: Option<String>,
    pub runtime_status: RuntimeStatus,
    pub error_kind: Option<ErrorKind>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AllocationView {
    pub id: String,
    pub protocol: Protocol,
    pub port: u16,
    pub target_port: Option<u16>,
    pub host: Option<String>,
    pub effective_target_port: Option<u16>,
    pub effective_host: Option<String>,
    pub host_configured: bool,
    pub runtime_status: RuntimeStatus,
    pub error_kind: Option<ErrorKind>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

pub fn is_host_configured(host: Option<&str>) -> bool {
    host.is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_parser_accepts_current_values_case_insensitively() {
        assert_eq!("tcp".parse::<Protocol>().unwrap(), Protocol::Tcp);
        assert_eq!("UDP".parse::<Protocol>().unwrap(), Protocol::Udp);
        assert_eq!("Both".parse::<Protocol>().unwrap(), Protocol::Both);
        assert!("http".parse::<Protocol>().is_err());
    }

    #[test]
    fn host_configured_helper_matches_legacy_semantics() {
        assert!(!is_host_configured(None));
        assert!(!is_host_configured(Some("")));
        assert!(is_host_configured(Some("127.0.0.1")));
    }
}
