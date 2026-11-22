use alice_core::error::HdbError;
use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::RwLock;

pub const DEFAULT_ITERATIONS: u32 = 100_000;
const NONCE_LEN: usize = 24;
const SALT_LEN: usize = 16;
const SHA256_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct ScramCredential {
    pub salt: [u8; SALT_LEN],
    pub iterations: u32,
    pub stored_key: [u8; SHA256_LEN],
    pub server_key: [u8; SHA256_LEN],
}

/// Thread-safe user credential registry for SCRAM-SHA-256 authentication.
/// When no users are registered, authentication is disabled (open mode).
#[derive(Default)]
pub struct AuthRegistry {
    users: RwLock<HashMap<String, ScramCredential>>,
}

impl AuthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> Result<bool, HdbError> {
        let users = self.users.read().map_err(|_| HdbError::LockPoisoned)?;
        Ok(!users.is_empty())
    }

    /// Registers a user by deriving SCRAM verifier keys from `password` (never stores plaintext).
    pub fn add_user(&self, username: impl Into<String>, password: &str) -> Result<(), HdbError> {
        let mut salt = [0u8; SALT_LEN];
        rand::fill(&mut salt).map_err(|_| HdbError::CorruptedData("CSPRNG failure"))?;

        let (stored_key, server_key) = derive_scram_keys(password, &salt, DEFAULT_ITERATIONS)?;
        let cred = ScramCredential {
            salt,
            iterations: DEFAULT_ITERATIONS,
            stored_key,
            server_key,
        };

        let mut users = self.users.write().map_err(|_| HdbError::LockPoisoned)?;
        users.insert(username.into(), cred);
        Ok(())
    }

    pub fn get_user(&self, username: &str) -> Result<Option<ScramCredential>, HdbError> {
        let users = self.users.read().map_err(|_| HdbError::LockPoisoned)?;
        Ok(users.get(username).cloned())
    }
}

pub fn generate_nonce() -> Result<[u8; NONCE_LEN], HdbError> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::fill(&mut nonce).map_err(|_| HdbError::CorruptedData("CSPRNG failure"))?;
    Ok(nonce)
}

pub fn derive_scram_keys(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<([u8; SHA256_LEN], [u8; SHA256_LEN]), HdbError> {
    let iter = NonZeroU32::new(iterations)
        .ok_or(HdbError::CorruptedData("PBKDF2 iterations must be > 0"))?;

    let mut salted_password = [0u8; SHA256_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iter,
        salt,
        password.as_bytes(),
        &mut salted_password,
    );

    let sp_key = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
    let client_key_tag = hmac::sign(&sp_key, b"Client Key");
    let server_key_tag = hmac::sign(&sp_key, b"Server Key");

    let stored_digest = digest::digest(&digest::SHA256, client_key_tag.as_ref());

    let mut stored_key = [0u8; SHA256_LEN];
    stored_key.copy_from_slice(stored_digest.as_ref());

    let mut server_key = [0u8; SHA256_LEN];
    server_key.copy_from_slice(server_key_tag.as_ref());

    Ok((stored_key, server_key))
}

pub fn build_auth_message(username: &str, combined_nonce: &[u8], salt: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(username.len() + combined_nonce.len() + salt.len() + 2);
    msg.extend_from_slice(username.as_bytes());
    msg.push(b',');
    msg.extend_from_slice(combined_nonce);
    msg.push(b',');
    msg.extend_from_slice(salt);
    msg
}

/// Computes client proof and expected server signature for SCRAM-SHA-256 verification.
pub fn compute_client_proof(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> Result<([u8; SHA256_LEN], [u8; SHA256_LEN]), HdbError> {
    let iter = NonZeroU32::new(iterations)
        .ok_or(HdbError::CorruptedData("PBKDF2 iterations must be > 0"))?;

    let mut salted_password = [0u8; SHA256_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iter,
        salt,
        password.as_bytes(),
        &mut salted_password,
    );

    let sp_key = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
    let client_key_tag = hmac::sign(&sp_key, b"Client Key");
    let server_key_tag = hmac::sign(&sp_key, b"Server Key");

    let stored_digest = digest::digest(&digest::SHA256, client_key_tag.as_ref());
    let stored_hmac_key = hmac::Key::new(hmac::HMAC_SHA256, stored_digest.as_ref());
    let client_sig = hmac::sign(&stored_hmac_key, auth_message);

    let mut proof = [0u8; SHA256_LEN];
    for (p, (&ck, &cs)) in proof
        .iter_mut()
        .zip(client_key_tag.as_ref().iter().zip(client_sig.as_ref().iter()))
    {
        *p = ck ^ cs;
    }

    let server_hmac_key = hmac::Key::new(hmac::HMAC_SHA256, server_key_tag.as_ref());
    let expected_server_sig_tag = hmac::sign(&server_hmac_key, auth_message);
    let mut expected_server_sig = [0u8; SHA256_LEN];
    expected_server_sig.copy_from_slice(expected_server_sig_tag.as_ref());

    Ok((proof, expected_server_sig))
}

/// Verifies a client's proof on the server and returns `ServerSignature` on success.
pub fn verify_client_proof(
    cred: &ScramCredential,
    auth_message: &[u8],
    client_proof: &[u8; SHA256_LEN],
) -> Option<[u8; SHA256_LEN]> {
    let stored_hmac_key = hmac::Key::new(hmac::HMAC_SHA256, &cred.stored_key);
    let client_sig = hmac::sign(&stored_hmac_key, auth_message);

    let mut recovered_client_key = [0u8; SHA256_LEN];
    for (rk, (&cp, &cs)) in recovered_client_key
        .iter_mut()
        .zip(client_proof.iter().zip(client_sig.as_ref().iter()))
    {
        *rk = cp ^ cs;
    }

    let recovered_stored = digest::digest(&digest::SHA256, &recovered_client_key);

    // Constant-time comparison to prevent timing side-channel attacks
    let mut diff: u8 = 0;
    for (a, b) in recovered_stored.as_ref().iter().zip(cred.stored_key.iter()) {
        diff |= a ^ b;
    }
    if diff != 0 {
        return None;
    }

    let server_hmac_key = hmac::Key::new(hmac::HMAC_SHA256, &cred.server_key);
    let server_sig_tag = hmac::sign(&server_hmac_key, auth_message);
    let mut server_sig = [0u8; SHA256_LEN];
    server_sig.copy_from_slice(server_sig_tag.as_ref());
    Some(server_sig)
}
