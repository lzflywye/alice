use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use common::DbError;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::RwLock;

pub const DEFAULT_ITERATIONS: u32 = 10_000;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 16;

#[derive(Debug, Clone)]
pub struct StoredCredential {
    pub salt: [u8; SALT_LEN],
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

#[derive(Default)]
pub struct AuthRegistry {
    users: RwLock<HashMap<String, StoredCredential>>,
}

impl AuthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_user(&self, username: &str, password: &str) -> Result<(), DbError> {
        let mut salt = [0u8; SALT_LEN];
        rand::fill(&mut salt).map_err(|_| DbError::CorruptedData("RNG failure"))?;

        let salted_password = derive_salted_password(password, &salt, DEFAULT_ITERATIONS)?;
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let stored_key = sha256(&client_key);

        let cred = StoredCredential {
            salt,
            iterations: DEFAULT_ITERATIONS,
            stored_key,
            server_key,
        };

        let mut guard = self.users.write().map_err(|_| DbError::LockPoisoned)?;
        guard.insert(username.to_string(), cred);
        Ok(())
    }

    pub fn is_enabled(&self) -> Result<bool, DbError> {
        let guard = self.users.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(!guard.is_empty())
    }

    pub fn get_user(&self, username: &str) -> Result<Option<StoredCredential>, DbError> {
        let guard = self.users.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(guard.get(username).cloned())
    }
}

pub fn generate_nonce() -> Result<[u8; NONCE_LEN], DbError> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::fill(&mut nonce).map_err(|_| DbError::CorruptedData("RNG failure"))?;
    Ok(nonce)
}

pub fn build_auth_message(username: &str, combined_nonce: &[u8], salt: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(username.len() + combined_nonce.len() + salt.len() + 8);
    msg.extend_from_slice(b"n=");
    msg.extend_from_slice(username.as_bytes());
    msg.extend_from_slice(b",r=");
    msg.extend_from_slice(combined_nonce);
    msg.extend_from_slice(b",s=");
    msg.extend_from_slice(salt);
    msg
}

pub fn compute_client_proof(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> Result<([u8; 32], [u8; 32]), DbError> {
    let salted_password = derive_salted_password(password, salt, iterations)?;
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let server_key = hmac_sha256(&salted_password, b"Server Key");
    let stored_key = sha256(&client_key);

    let client_sig = hmac_sha256(&stored_key, auth_message);
    let server_sig = hmac_sha256(&server_key, auth_message);

    let mut client_proof = [0u8; 32];
    for i in 0..32 {
        client_proof[i] = client_key[i] ^ client_sig[i];
    }
    Ok((client_proof, server_sig))
}

pub fn verify_client_proof(
    cred: &StoredCredential,
    auth_message: &[u8],
    client_proof: &[u8],
) -> Option<[u8; 32]> {
    if client_proof.len() != 32 {
        return None;
    }
    let client_sig = hmac_sha256(&cred.stored_key, auth_message);
    let mut recovered_client_key = [0u8; 32];
    for i in 0..32 {
        recovered_client_key[i] = client_proof[i] ^ client_sig[i];
    }
    let computed_stored_key = sha256(&recovered_client_key);
    let mut diff: u8 = 0;
    for (a, b) in computed_stored_key.iter().zip(cred.stored_key.iter()) {
        diff |= a ^ b;
    }
    if diff != 0 {
        return None;
    }
    Some(hmac_sha256(&cred.server_key, auth_message))
}

fn derive_salted_password(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<[u8; 32], DbError> {
    let iter = NonZeroU32::new(iterations.max(1))
        .ok_or(DbError::CorruptedData("Invalid PBKDF2 iterations"))?;
    let mut out = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iter,
        salt,
        password.as_bytes(),
        &mut out,
    );
    Ok(out)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&s_key, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let d = digest::digest(&digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}
