use crate::configs::http::HttpJwtConfig;
use crate::http::state::AppState;
use crate::streaming::utils::hash;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use iggy::error::Error;
use iggy::models::user_info::UserId;
use iggy::utils::text::as_base64;
use iggy::utils::timestamp::TimeStamp;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation};
use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

const REFRESH_TOKEN_SIZE: usize = 50;
const AUTHORIZATION: &str = "authorization";
const BEARER: &str = "Bearer ";
const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;
const REVOKED_ACCESS_TOKENS_KEY_PREFIX: &str = "revoked_access_token";
const REFRESH_TOKENS_KEY_PREFIX: &str = "refresh_token";

#[derive(Debug, Clone)]
pub struct Identity {
    pub token_id: String,
    pub token_expiry: u64,
    pub user_id: UserId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub jti: String,
    pub iss: String,
    pub aud: String,
    pub sub: u32,
    pub iat: u64,
    pub exp: u64,
    pub nbf: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevokedAccessToken {
    pub id: String,
    pub expiry: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshToken {
    #[serde(skip)]
    pub hash: String,
    pub user_id: u32,
    pub expiry: u64,
}

impl RefreshToken {
    pub fn new(user_id: UserId, now: u64, expiry: u64) -> (Self, String) {
        let mut buffer: [u8; REFRESH_TOKEN_SIZE] = [0; REFRESH_TOKEN_SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        let hash = hash::calculate_256(token.as_bytes());
        let expiry = now + expiry;
        (
            Self {
                hash,
                user_id,
                expiry,
            },
            token,
        )
    }
}

#[derive(Debug)]
pub struct GeneratedTokens {
    pub user_id: UserId,
    pub access_token: String,
    pub refresh_token: String,
    pub expiry: u64,
}

pub async fn jwt_auth<T>(
    State(state): State<Arc<AppState>>,
    mut request: Request<T>,
    next: Next<T>,
) -> Result<Response, StatusCode> {
    if should_skip_auth(request.uri().path()) {
        return Ok(next.run(request).await);
    }

    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .ok_or(UNAUTHORIZED)?
        .to_str()
        .map_err(|_| UNAUTHORIZED)?;

    if !bearer.starts_with(BEARER) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let jwt_token = &bearer[BEARER.len()..];
    let token_header = jsonwebtoken::decode_header(jwt_token).map_err(|_| UNAUTHORIZED)?;
    let jwt_claims = state
        .jwt_manager
        .decode(jwt_token, token_header.alg)
        .map_err(|_| UNAUTHORIZED)?;
    if state
        .jwt_manager
        .is_token_revoked(&jwt_claims.claims.jti)
        .await
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let identity = Identity {
        token_id: jwt_claims.claims.jti,
        token_expiry: jwt_claims.claims.exp,
        user_id: jwt_claims.claims.sub,
    };
    request.extensions_mut().insert(identity);
    Ok(next.run(request).await)
}

#[derive(Debug)]
pub struct TokensStorage {
    db: Arc<Db>,
}

impl TokensStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    pub fn load_refresh_token(&self, token_hash: &str) -> Result<RefreshToken, Error> {
        let key = Self::get_refresh_token_key(token_hash);
        let token_data = self.db.get(&key);
        if token_data.is_err() {
            return Err(Error::CannotLoadResource(key));
        }

        let token_data = token_data.unwrap();
        if token_data.is_none() {
            return Err(Error::CannotLoadResource(key));
        }

        let token_data = token_data.unwrap();
        let token_data = rmp_serde::from_slice::<RefreshToken>(&token_data);
        if token_data.is_err() {
            return Err(Error::CannotDeserializeResource(key));
        }

        let mut token_data = token_data.unwrap();
        token_data.hash = token_hash.to_string();
        Ok(token_data)
    }

    pub fn load_all_refresh_tokens(&self) -> Result<Vec<RefreshToken>, Error> {
        let mut refresh_tokens = Vec::new();
        let key = format!("{REFRESH_TOKENS_KEY_PREFIX}:");
        for data in self.db.scan_prefix(&key) {
            let token = match data {
                Ok((hash, value)) => match rmp_serde::from_slice::<RefreshToken>(&value) {
                    Ok(mut token) => {
                        token.hash = from_utf8(&hash)?.to_string();
                        token
                    }
                    Err(err) => {
                        error!("Cannot deserialize refresh token. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(key.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load refresh token. Error: {}", err);
                    return Err(Error::CannotLoadResource(key.to_string()));
                }
            };
            refresh_tokens.push(token);
        }
        info!("Loaded {} refresh tokens", refresh_tokens.len());
        Ok(refresh_tokens)
    }

    pub fn load_all_revoked_access_tokens(&self) -> Result<Vec<RevokedAccessToken>, Error> {
        let mut revoked_tokens = Vec::new();
        let key = format!("{REVOKED_ACCESS_TOKENS_KEY_PREFIX}:");
        for data in self.db.scan_prefix(&key) {
            let token = match data {
                Ok((_, value)) => match rmp_serde::from_slice::<RevokedAccessToken>(&value) {
                    Ok(token) => token,
                    Err(err) => {
                        error!("Cannot deserialize revoked access token. Error: {}", err);
                        return Err(Error::CannotDeserializeResource(key.to_string()));
                    }
                },
                Err(err) => {
                    error!("Cannot load revoked access token. Error: {}", err);
                    return Err(Error::CannotLoadResource(key.to_string()));
                }
            };
            revoked_tokens.push(token);
        }
        info!("Loaded {} revoked access tokens", revoked_tokens.len());
        Ok(revoked_tokens)
    }

    pub fn save_revoked_access_token(&self, token: &RevokedAccessToken) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(&token.id);
        match rmp_serde::to_vec(&token) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save revoked access token. Error: {err}");
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize revoked access token. Error: {err}");
                return Err(Error::CannotSerializeResource(key));
            }
        }
        Ok(())
    }

    pub fn save_refresh_token(&self, token_hash: &str, token: &RefreshToken) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(token_hash);
        match rmp_serde::to_vec(&token) {
            Ok(data) => {
                if let Err(err) = self.db.insert(&key, data) {
                    error!("Cannot save refresh token. Error: {err}");
                    return Err(Error::CannotSaveResource(key.to_string()));
                }
            }
            Err(err) => {
                error!("Cannot serialize refresh token. Error: {err}");
                return Err(Error::CannotSerializeResource(key));
            }
        }
        Ok(())
    }

    pub fn delete_revoked_access_token(&self, id: &str) -> Result<(), Error> {
        let key = Self::get_revoked_token_key(id);
        if let Err(err) = self.db.remove(&key) {
            error!("Cannot delete revoked access token. Error: {err}");
            return Err(Error::CannotDeleteResource(key.to_string()));
        }
        Ok(())
    }

    pub fn delete_refresh_token(&self, token_hash: &str) -> Result<(), Error> {
        let key = Self::get_refresh_token_key(token_hash);
        if let Err(err) = self.db.remove(&key) {
            error!("Cannot delete refresh token. Error: {err}");
            return Err(Error::CannotDeleteResource(key.to_string()));
        }
        Ok(())
    }

    fn get_revoked_token_key(id: &str) -> String {
        format!("{REVOKED_ACCESS_TOKENS_KEY_PREFIX}:{id}")
    }

    fn get_refresh_token_key(token_hash: &str) -> String {
        format!("{REFRESH_TOKENS_KEY_PREFIX}:{token_hash}")
    }
}

pub struct IssuerOptions {
    pub issuer: String,
    pub audience: String,
    pub access_token_expiry: u64,
    pub refresh_token_expiry: u64,
    pub not_before: u64,
    pub key: EncodingKey,
    pub algorithm: Algorithm,
}

pub struct ValidatorOptions {
    pub valid_audiences: Vec<String>,
    pub valid_issuers: Vec<String>,
    pub clock_skew: u64,
    pub key: DecodingKey,
}

pub struct JwtManager {
    issuer: IssuerOptions,
    validator: ValidatorOptions,
    tokens_storage: TokensStorage,
    revoked_tokens: RwLock<HashMap<String, u64>>,
    validations: HashMap<Algorithm, Validation>,
}

impl JwtManager {
    pub fn new(
        issuer: IssuerOptions,
        validator: ValidatorOptions,
        db: Arc<Db>,
    ) -> Result<Self, Error> {
        let validation = JwtManager::create_validation(
            issuer.algorithm,
            &validator.valid_issuers,
            &validator.valid_audiences,
            validator.clock_skew,
        );

        Ok(Self {
            validations: vec![(issuer.algorithm, validation)].into_iter().collect(),
            issuer,
            validator,
            tokens_storage: TokensStorage::new(db),
            revoked_tokens: RwLock::new(HashMap::new()),
        })
    }

    pub fn from_config(config: &HttpJwtConfig, db: Arc<Db>) -> Result<Self, Error> {
        let algorithm = config.get_algorithm()?;
        let issuer = IssuerOptions {
            issuer: config.issuer.clone(),
            audience: config.audience.clone(),
            access_token_expiry: config.access_token_expiry,
            refresh_token_expiry: config.refresh_token_expiry,
            not_before: config.not_before,
            key: config.get_encoding_key()?,
            algorithm,
        };
        let validator = ValidatorOptions {
            valid_audiences: config.valid_audiences.clone(),
            valid_issuers: config.valid_issuers.clone(),
            clock_skew: config.clock_skew,
            key: config.get_decoding_key()?,
        };
        JwtManager::new(issuer, validator, db)
    }

    fn create_validation(
        algorithm: Algorithm,
        issuers: &[String],
        audiences: &[String],
        clock_skew: u64,
    ) -> Validation {
        let mut validator = Validation::new(algorithm);
        validator.set_issuer(issuers);
        validator.set_audience(audiences);
        validator.leeway = clock_skew;
        validator
    }

    pub async fn load_revoked_tokens(&self) -> Result<(), Error> {
        let revoked_tokens = self.tokens_storage.load_all_revoked_access_tokens()?;
        let mut tokens = self.revoked_tokens.write().await;
        for token in revoked_tokens {
            tokens.insert(token.id, token.expiry);
        }
        Ok(())
    }

    pub async fn delete_expired_revoked_tokens(&self, now: u64) -> Result<(), Error> {
        let mut tokens_to_delete = Vec::new();
        let revoked_tokens = self.revoked_tokens.read().await;
        for (id, expiry) in revoked_tokens.iter() {
            if expiry < &now {
                tokens_to_delete.push(id.to_string());
            }
        }
        drop(revoked_tokens);

        debug!(
            "Found {} expired revoked access tokens to delete.",
            tokens_to_delete.len()
        );
        if tokens_to_delete.is_empty() {
            return Ok(());
        }

        debug!(
            "Deleting {} expired revoked access tokens...",
            tokens_to_delete.len()
        );
        let mut revoked_tokens = self.revoked_tokens.write().await;
        for id in tokens_to_delete {
            revoked_tokens.remove(&id);
            self.tokens_storage.delete_revoked_access_token(&id)?;
            debug!("Deleted expired revoked access token with ID: {id}")
        }

        Ok(())
    }

    pub async fn delete_expired_refresh_tokens(&self, now: u64) -> Result<(), Error> {
        let mut tokens_to_delete = Vec::new();
        let refresh_tokens = self.tokens_storage.load_all_refresh_tokens()?;
        for token in refresh_tokens {
            if token.expiry < now {
                tokens_to_delete.push(token.hash);
            }
        }

        debug!(
            "Found {} expired refresh tokens to delete.",
            tokens_to_delete.len()
        );
        if tokens_to_delete.is_empty() {
            return Ok(());
        }

        debug!(
            "Deleting {} expired refresh tokens...",
            tokens_to_delete.len()
        );
        for token_hash in tokens_to_delete {
            self.tokens_storage.delete_refresh_token(&token_hash)?;
            debug!("Deleted expired refresh token with hash: {token_hash}")
        }

        Ok(())
    }

    pub fn generate(&self, user_id: UserId) -> Result<GeneratedTokens, Error> {
        let header = Header::new(self.issuer.algorithm);
        let now = TimeStamp::now().to_secs();
        let iat = now;
        let exp = iat + self.issuer.access_token_expiry;
        let nbf = iat + self.issuer.not_before;
        let claims = JwtClaims {
            jti: uuid::Uuid::new_v4().to_string(),
            sub: user_id,
            aud: self.issuer.audience.to_string(),
            iss: self.issuer.issuer.to_string(),
            iat,
            exp,
            nbf,
        };

        let access_token = encode::<JwtClaims>(&header, &claims, &self.issuer.key);
        if let Err(err) = access_token {
            error!("Cannot generate JWT token. Error: {}", err);
            return Err(Error::CannotGenerateJwt);
        }

        let (refresh_token, raw_refresh_token) =
            RefreshToken::new(user_id, now, self.issuer.refresh_token_expiry);
        let refresh_token_hash = hash::calculate_256(raw_refresh_token.as_bytes());
        self.tokens_storage
            .save_refresh_token(&refresh_token_hash, &refresh_token)?;

        Ok(GeneratedTokens {
            user_id,
            access_token: access_token.unwrap(),
            refresh_token: raw_refresh_token,
            expiry: exp,
        })
    }

    pub fn refresh_token(&self, refresh_token: &str) -> Result<GeneratedTokens, Error> {
        let now = TimeStamp::now().to_secs();
        if refresh_token.is_empty() {
            return Err(Error::InvalidRefreshToken);
        }

        let token_hash = hash::calculate_256(refresh_token.as_bytes());
        let refresh_token = self.tokens_storage.load_refresh_token(&token_hash);
        if refresh_token.is_err() {
            return Err(Error::InvalidRefreshToken);
        }

        let refresh_token = refresh_token.unwrap();
        self.tokens_storage.delete_refresh_token(&token_hash)?;
        if refresh_token.expiry < now {
            return Err(Error::RefreshTokenExpired);
        }

        self.generate(refresh_token.user_id)
    }

    pub fn decode(&self, token: &str, algorithm: Algorithm) -> Result<TokenData<JwtClaims>, Error> {
        let validation = self.validations.get(&algorithm);
        if validation.is_none() {
            return Err(Error::InvalidJwtAlgorithm(Self::map_algorithm_to_string(
                algorithm,
            )));
        }

        let validation = validation.unwrap();
        match jsonwebtoken::decode::<JwtClaims>(token, &self.validator.key, validation) {
            Ok(claims) => Ok(claims),
            _ => Err(Error::Unauthenticated),
        }
    }

    fn map_algorithm_to_string(algorithm: Algorithm) -> String {
        match algorithm {
            Algorithm::HS256 => "HS256",
            Algorithm::HS384 => "HS384",
            Algorithm::HS512 => "HS512",
            Algorithm::RS256 => "RS256",
            Algorithm::RS384 => "RS384",
            Algorithm::RS512 => "RS512",
            _ => "Unknown",
        }
        .to_string()
    }

    pub async fn revoke_token(&self, token_id: &str, expiry: u64) -> Result<(), Error> {
        let mut revoked_tokens = self.revoked_tokens.write().await;
        revoked_tokens.insert(token_id.to_string(), expiry);
        self.tokens_storage
            .save_revoked_access_token(&RevokedAccessToken {
                id: token_id.to_string(),
                expiry,
            })?;
        info!("Revoked access token with ID: {token_id}");
        Ok(())
    }

    pub async fn is_token_revoked(&self, token_id: &str) -> bool {
        let revoked_tokens = self.revoked_tokens.read().await;
        revoked_tokens.contains_key(token_id)
    }
}

fn should_skip_auth(path: &str) -> bool {
    matches!(
        path,
        "/" | "/metrics"
            | "/ping"
            | "/users/login"
            | "/users/refresh-token"
            | "/personal-access-tokens/login"
    )
}
