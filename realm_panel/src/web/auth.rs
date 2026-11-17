use std::sync::Arc;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand_core::{OsRng, RngCore};
use argon2::Argon2;
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::storage::Storage;
use crate::web::AppState;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct AuthService {
    storage: Storage,
    secret: Vec<u8>,
}

impl AuthService {
    pub fn init(storage: Storage, admin_user: &str, admin_pass: Option<&str>) -> anyhow::Result<Self> {
        let secret = match storage.get_setting("auth_secret")? {
            Some(hex_str) => hex::decode(hex_str)?,
            None => {
                let mut key = vec![0u8; 32];
                OsRng.fill_bytes(&mut key);
                storage.set_setting("auth_secret", &hex::encode(&key))?;
                key
            }
        };

        if !storage.has_any_user()? {
            let pass = match admin_pass {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    let mut raw = [0u8; 12];
                    OsRng.fill_bytes(&mut raw);
                    let generated = hex::encode(raw);
                    println!("============================================================");
                    println!("[realm-panel] Initialized admin credentials:");
                    println!("  Username: {}", admin_user);
                    println!("  Password: {}", generated);
                    println!("============================================================");
                    generated
                }
            };
            let hash = hash_password(&pass)?;
            storage.upsert_user(admin_user, &hash)?;
        }

        Ok(Self { storage, secret })
    }

    pub fn verify_login(&self, username: &str, password: &str) -> anyhow::Result<Option<String>> {
        let Some(stored_hash) = self.storage.get_user_hash(username)? else {
            return Ok(None);
        };
        let parsed = PasswordHash::new(&stored_hash)
            .map_err(|e| anyhow::anyhow!("invalid password hash: {e}"))?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
        {
            Ok(Some(self.issue_token(username)))
        } else {
            Ok(None)
        }
    }

    pub fn change_password(&self, username: &str, new_password: &str) -> anyhow::Result<()> {
        let hash = hash_password(new_password)?;
        self.storage.upsert_user(username, &hash)?;
        Ok(())
    }

    pub fn issue_token(&self, username: &str) -> String {
        let exp = Utc::now().timestamp() + 7 * 86400;
        let payload = format!("{}:{}", username, exp);
        let sig = self.sign(&payload);
        format!("{}:{}", payload, sig)
    }

    pub fn verify_token(&self, token: &str) -> Option<String> {
        let parts: Vec<&str> = token.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }
        let sig = parts[0];
        let payload = parts[1];
        if self.sign(payload) != sig {
            return None;
        }
        let (username, exp_str) = payload.rsplit_once(':')?;
        let exp: i64 = exp_str.parse().ok()?;
        if Utc::now().timestamp() > exp {
            return None;
        }
        Some(username.to_string())
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts any key size");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("password hash failed: {e}"))?;
    Ok(hash.to_string())
}

pub async fn require_auth(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token_from_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .map(ToString::to_string);

    let token_from_query = req.uri().query().and_then(|q| {
        q.split('&')
            .find_map(|pair| pair.strip_prefix("token="))
            .map(ToString::to_string)
    });

    let token = token_from_header.or(token_from_query);
    let Some(t) = token else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let Some(username) = state.auth.verify_token(&t) else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    req.extensions_mut().insert(username);
    Ok(next.run(req).await)
}
