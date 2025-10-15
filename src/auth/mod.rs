use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::protocol::redis::{RedisCommand, RespCodec, RespValue};

/// Redis 默认用户名。
pub const DEFAULT_USER: &str = "default";

/// 前端 ACL 配置，兼容旧版简单密码写法。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FrontendAuthConfig {
    /// 旧版 `password = "xxx"` 样式。
    Password(String),
    /// 新版 ACL 配置。
    Detailed(FrontendAuthTable),
}

#[derive(Debug, Clone, Deserialize)]
pub struct FrontendAuthTable {
    /// 简化写法：纯密码等价于 default 用户。
    #[serde(default)]
    pub password: Option<String>,
    /// ACL 用户列表。
    #[serde(default)]
    pub users: Vec<AuthUserConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthUserConfig {
    pub username: String,
    pub password: String,
}

impl FrontendAuthConfig {
    pub fn into_users(self) -> Vec<AuthUserConfig> {
        match self {
            FrontendAuthConfig::Password(password) => vec![AuthUserConfig {
                username: DEFAULT_USER.to_string(),
                password,
            }],
            FrontendAuthConfig::Detailed(table) => {
                let mut users = Vec::new();
                if let Some(password) = table.password {
                    users.push(AuthUserConfig {
                        username: DEFAULT_USER.to_string(),
                        password,
                    });
                }
                users.extend(table.users);
                users
            }
        }
    }
}

/// 后端认证配置，支持 ACL 写法与旧式密码。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BackendAuthConfig {
    Password(String),
    Credential { username: String, password: String },
}

#[derive(Debug, Clone)]
pub struct BackendAuth {
    parts: Arc<[Bytes]>,
}

impl BackendAuth {
    pub fn from_password(password: String) -> Self {
        let parts = vec![Bytes::from_static(b"AUTH"), Bytes::from(password)];
        Self {
            parts: parts.into(),
        }
    }

    pub fn from_credential(username: String, password: String) -> Self {
        let parts = vec![
            Bytes::from_static(b"AUTH"),
            Bytes::from(username),
            Bytes::from(password),
        ];
        Self {
            parts: parts.into(),
        }
    }

    pub fn command(&self) -> RedisCommand {
        RedisCommand::new(self.parts.iter().cloned().collect())
            .expect("AUTH command is always valid")
    }

    pub async fn apply_to_stream(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        timeout_duration: Duration,
        target: &str,
    ) -> Result<()> {
        let command = self.command();
        timeout(timeout_duration, framed.send(command.to_resp()))
            .await
            .with_context(|| format!("timed out sending AUTH to {}", target))??;

        let response = timeout(timeout_duration, framed.next())
            .await
            .with_context(|| format!("timed out waiting for AUTH reply from {}", target))?
            .ok_or_else(|| anyhow!("backend {} closed connection during AUTH", target))?;

        let frame = response?;

        match frame {
            RespValue::SimpleString(ref data) | RespValue::BulkString(ref data)
                if data.eq_ignore_ascii_case(b"OK") =>
            {
                Ok(())
            }
            RespValue::Error(err) => Err(anyhow!(
                "backend {} rejected AUTH: {}",
                target,
                String::from_utf8_lossy(&err)
            )),
            other => Err(anyhow!(
                "unexpected response for backend {} AUTH: {:?}",
                target,
                other
            )),
        }
    }
}

impl From<BackendAuthConfig> for BackendAuth {
    fn from(value: BackendAuthConfig) -> Self {
        match value {
            BackendAuthConfig::Password(password) => BackendAuth::from_password(password),
            BackendAuthConfig::Credential { username, password } => {
                BackendAuth::from_credential(username, password)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct AuthUser {
    secret: Arc<[u8]>,
}

impl From<AuthUserConfig> for AuthUser {
    fn from(value: AuthUserConfig) -> Self {
        Self {
            secret: Arc::from(value.password.into_bytes()),
        }
    }
}

/// 前端认证器，按连接维护状态。
#[derive(Debug, Clone)]
pub struct FrontendAuthenticator {
    users: Arc<HashMap<String, AuthUser>>,
}

impl FrontendAuthenticator {
    pub fn from_users(users: Vec<AuthUserConfig>) -> Result<Self> {
        if users.is_empty() {
            bail!("auth configuration must declare at least one user");
        }
        let mut map = HashMap::new();
        for user_cfg in users.into_iter() {
            let username = user_cfg.username.clone();
            let user = AuthUser::from(user_cfg);
            if map.insert(username.clone(), user).is_some() {
                bail!("duplicate user '{}' in auth configuration", username);
            }
        }
        Ok(Self {
            users: Arc::new(map),
        })
    }

    pub fn new_session(&self) -> ClientAuthState {
        ClientAuthState {
            authenticated_user: None,
        }
    }

    pub fn handle_command(
        &self,
        state: &mut ClientAuthState,
        command: &RedisCommand,
    ) -> AuthAction {
        let name = command.command_name();
        if name.eq_ignore_ascii_case(b"AUTH") {
            return self.process_auth(state, command);
        }
        if name.eq_ignore_ascii_case(b"HELLO") {
            return self.process_hello(state, command);
        }

        if state.is_authenticated() {
            AuthAction::Allow
        } else {
            AuthAction::Reply(noauth_error())
        }
    }

    fn process_auth(&self, state: &mut ClientAuthState, command: &RedisCommand) -> AuthAction {
        let args = command.args();
        match args.len() {
            2 => match self.users.get(DEFAULT_USER) {
                Some(user) => self.finalize_auth(state, DEFAULT_USER, &user.secret, &args[1]),
                None => AuthAction::Reply(invalid_default_user_error()),
            },
            3 => {
                let username = match std::str::from_utf8(&args[1]) {
                    Ok(name) => name,
                    Err(_) => return AuthAction::Reply(invalid_username_error()),
                };
                match self.users.get(username) {
                    Some(user) => self.finalize_auth(state, username, &user.secret, &args[2]),
                    None => AuthAction::Reply(unknown_user_error()),
                }
            }
            _ => AuthAction::Reply(wrong_number_args_error("AUTH")),
        }
    }

    fn process_hello(&self, state: &mut ClientAuthState, command: &RedisCommand) -> AuthAction {
        match extract_hello_credentials(command.args()) {
            Ok(Some((username, password, _auth_index))) => {
                match self.users.get(username.as_str()) {
                    Some(user) => {
                        if secrets_match(&user.secret, &password) {
                            state.authenticated_user = Some(username);
                            let rewritten = strip_hello_auth(command);
                            AuthAction::Rewrite(rewritten)
                        } else {
                            AuthAction::Reply(wrongpass_error())
                        }
                    }
                    None => AuthAction::Reply(unknown_user_error()),
                }
            }
            Ok(None) => {
                if state.is_authenticated() {
                    AuthAction::Allow
                } else {
                    AuthAction::Reply(noauth_error())
                }
            }
            Err(resp) => AuthAction::Reply(resp),
        }
    }

    fn finalize_auth(
        &self,
        state: &mut ClientAuthState,
        username: &str,
        secret: &[u8],
        supplied: &Bytes,
    ) -> AuthAction {
        if secrets_match(secret, supplied) {
            state.authenticated_user = Some(username.to_string());
            AuthAction::Reply(ok_response())
        } else {
            AuthAction::Reply(wrongpass_error())
        }
    }
}

pub struct ClientAuthState {
    authenticated_user: Option<String>,
}

impl ClientAuthState {
    fn is_authenticated(&self) -> bool {
        self.authenticated_user.is_some()
    }
}

#[derive(Debug)]
pub enum AuthAction {
    Allow,
    Rewrite(RedisCommand),
    Reply(RespValue),
}

fn extract_hello_credentials(args: &[Bytes]) -> Result<Option<(String, Bytes, usize)>, RespValue> {
    if args.len() <= 1 {
        return Ok(None);
    }
    let mut idx = 1;
    while idx < args.len() {
        let token = &args[idx];
        if token.eq_ignore_ascii_case(b"AUTH") {
            let username = args
                .get(idx + 1)
                .ok_or_else(|| wrong_number_args_error("HELLO"))?;
            let password = args
                .get(idx + 2)
                .ok_or_else(|| wrong_number_args_error("HELLO"))?
                .clone();
            let username = std::str::from_utf8(username)
                .map_err(|_| invalid_username_error())?
                .to_string();
            return Ok(Some((username, password, idx)));
        } else if token.eq_ignore_ascii_case(b"SETNAME") {
            idx += 2;
            continue;
        }
        idx += 1;
    }
    Ok(None)
}

fn secrets_match(expected: &[u8], supplied: &Bytes) -> bool {
    expected == supplied.as_ref()
}

fn strip_hello_auth(command: &RedisCommand) -> RedisCommand {
    let args = command.args();
    let mut parts = Vec::with_capacity(args.len());
    let mut idx = 0;
    while idx < args.len() {
        let part = &args[idx];
        if part.eq_ignore_ascii_case(b"AUTH") && idx + 2 < args.len() {
            idx += 3;
            continue;
        }
        parts.push(part.clone());
        idx += 1;
    }
    RedisCommand::new(parts).expect("HELLO command without AUTH remains valid")
}

fn ok_response() -> RespValue {
    RespValue::SimpleString(Bytes::from_static(b"OK"))
}

fn noauth_error() -> RespValue {
    RespValue::Error(Bytes::from_static(b"NOAUTH Authentication required."))
}

fn wrongpass_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"WRONGPASS invalid username-password pair",
    ))
}

fn unknown_user_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"WRONGPASS invalid username-password pair",
    ))
}

fn invalid_default_user_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"ERR AUTH called without username but no default user is configured",
    ))
}

fn invalid_username_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"ERR username must be a valid UTF-8 string",
    ))
}

fn wrong_number_args_error(command: &str) -> RespValue {
    RespValue::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        command.to_ascii_lowercase()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn legacy_auth() -> FrontendAuthenticator {
        FrontendAuthenticator::from_users(vec![AuthUserConfig {
            username: DEFAULT_USER.to_string(),
            password: "secret".to_string(),
        }])
        .expect("auth")
    }

    #[test]
    fn denies_commands_before_auth() {
        let auth = legacy_auth();
        let mut state = auth.new_session();
        let ping =
            RedisCommand::new(vec![Bytes::from_static(b"PING")]).expect("ping command created");
        match auth.handle_command(&mut state, &ping) {
            AuthAction::Reply(RespValue::Error(err)) => {
                assert_eq!(err.as_ref(), b"NOAUTH Authentication required.")
            }
            _ => panic!("expected NOAUTH error"),
        }
    }

    #[test]
    fn auth_without_username_uses_default_user() {
        let auth = legacy_auth();
        let mut state = auth.new_session();
        let auth_cmd = RedisCommand::new(vec![
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"secret"),
        ])
        .expect("auth command created");
        if let AuthAction::Reply(RespValue::SimpleString(value)) =
            auth.handle_command(&mut state, &auth_cmd)
        {
            assert_eq!(value.as_ref(), b"OK");
        } else {
            panic!("AUTH command did not return OK");
        }

        let ping =
            RedisCommand::new(vec![Bytes::from_static(b"PING")]).expect("ping command created");
        assert!(matches!(
            auth.handle_command(&mut state, &ping),
            AuthAction::Allow
        ));
    }

    #[test]
    fn hello_with_auth_grants_access() {
        let auth = legacy_auth();
        let mut state = auth.new_session();
        let hello = RedisCommand::new(vec![
            Bytes::from_static(b"HELLO"),
            Bytes::from_static(b"3"),
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"default"),
            Bytes::from_static(b"secret"),
        ])
        .expect("hello command created");
        match auth.handle_command(&mut state, &hello) {
            AuthAction::Rewrite(rewritten) => {
                let args = rewritten.args();
                assert!(args.iter().all(|part| !part.eq_ignore_ascii_case(b"AUTH")));
                assert!(state.is_authenticated());
            }
            other => panic!("unexpected response: {:?}", other),
        }

        let ping =
            RedisCommand::new(vec![Bytes::from_static(b"PING")]).expect("ping command created");
        assert!(matches!(
            auth.handle_command(&mut state, &ping),
            AuthAction::Allow
        ));
    }

    #[test]
    fn denies_unknown_or_wrong_credentials() {
        let auth = FrontendAuthenticator::from_users(vec![
            AuthUserConfig {
                username: DEFAULT_USER.to_string(),
                password: "secret".to_string(),
            },
            AuthUserConfig {
                username: "ops".to_string(),
                password: "ops-secret".to_string(),
            },
        ])
        .expect("auth");
        let mut state = auth.new_session();

        let wrong_user = RedisCommand::new(vec![
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"unknown"),
            Bytes::from_static(b"secret"),
        ])
        .expect("auth wrong user");
        assert!(matches!(
            auth.handle_command(&mut state, &wrong_user),
            AuthAction::Reply(RespValue::Error(err)) if err.as_ref() == b"WRONGPASS invalid username-password pair"
        ));

        let mut state_default = auth.new_session();
        let wrong_password = RedisCommand::new(vec![
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"secret2"),
        ])
        .expect("auth wrong pass");
        assert!(matches!(
            auth.handle_command(&mut state_default, &wrong_password),
            AuthAction::Reply(RespValue::Error(err)) if err.as_ref() == b"WRONGPASS invalid username-password pair"
        ));

        let mut state_ops = auth.new_session();
        let hello_wrong = RedisCommand::new(vec![
            Bytes::from_static(b"HELLO"),
            Bytes::from_static(b"3"),
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"ops"),
            Bytes::from_static(b"bad"),
        ])
        .expect("hello wrong pass");
        assert!(matches!(
            auth.handle_command(&mut state_ops, &hello_wrong),
            AuthAction::Reply(RespValue::Error(err)) if err.as_ref() == b"WRONGPASS invalid username-password pair"
        ));
    }
}
