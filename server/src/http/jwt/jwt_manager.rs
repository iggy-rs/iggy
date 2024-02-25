use crate::configs::http::HttpJwtConfig;
use crate::http::jwt::json_web_token::{GeneratedTokens, JwtClaims, RevokedAccessToken};
use crate::http::jwt::refresh_token::RefreshToken;
use crate::http::jwt::storage::TokenStorage;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::models::user_info::UserId;
use iggy::utils::duration::IggyDuration;
use iggy::utils::timestamp::IggyTimestamp;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation};
use sled::Db;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

pub struct IssuerOptions {
    pub issuer: String,
    pub audience: String,
    pub access_token_expiry: IggyDuration,
    pub refresh_token_expiry: IggyDuration,
    pub not_before: IggyDuration,
    pub key: EncodingKey,
    pub algorithm: Algorithm,
}

pub struct ValidatorOptions {
    pub valid_audiences: Vec<String>,
    pub valid_issuers: Vec<String>,
    pub clock_skew: IggyDuration,
    pub key: DecodingKey,
}

pub struct JwtManager {
    issuer: IssuerOptions,
    validator: ValidatorOptions,
    tokens_storage: TokenStorage,
    revoked_tokens: IggySharedMut<HashMap<String, u64>>,
    validations: HashMap<Algorithm, Validation>,
}

impl JwtManager {
    pub fn new(
        issuer: IssuerOptions,
        validator: ValidatorOptions,
        db: Arc<Db>,
    ) -> Result<Self, IggyError> {
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
            tokens_storage: TokenStorage::new(db),
            revoked_tokens: IggySharedMut::new(HashMap::new()),
        })
    }

    pub fn from_config(config: &HttpJwtConfig, db: Arc<Db>) -> Result<Self, IggyError> {
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
        clock_skew: IggyDuration,
    ) -> Validation {
        let mut validator = Validation::new(algorithm);
        validator.set_issuer(issuers);
        validator.set_audience(audiences);
        validator.leeway = clock_skew.as_secs() as u64;
        validator
    }

    pub async fn load_revoked_tokens(&self) -> Result<(), IggyError> {
        let revoked_tokens = self.tokens_storage.load_all_revoked_access_tokens()?;
        let mut tokens = self.revoked_tokens.write().await;
        for token in revoked_tokens {
            tokens.insert(token.id, token.expiry);
        }
        Ok(())
    }

    pub async fn delete_expired_revoked_tokens(&self, now: u64) -> Result<(), IggyError> {
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

    pub async fn delete_expired_refresh_tokens(&self, now: u64) -> Result<(), IggyError> {
        let mut tokens_to_delete = Vec::new();
        let refresh_tokens = self.tokens_storage.load_all_refresh_tokens()?;
        for token in refresh_tokens {
            if token.is_expired(now) {
                tokens_to_delete.push(token.token_hash);
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

    pub fn generate(&self, user_id: UserId) -> Result<GeneratedTokens, IggyError> {
        let header = Header::new(self.issuer.algorithm);
        let now = IggyTimestamp::now().to_secs();
        let iat = now;
        let exp = iat + self.issuer.access_token_expiry.as_secs() as u64;
        let nbf = iat + self.issuer.not_before.as_secs() as u64;
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
            return Err(IggyError::CannotGenerateJwt);
        }

        let (refresh_token, raw_refresh_token) = RefreshToken::new(
            user_id,
            now,
            self.issuer.refresh_token_expiry.as_secs() as u64,
        );
        self.tokens_storage.save_refresh_token(&refresh_token)?;

        Ok(GeneratedTokens {
            user_id,
            access_token: access_token.unwrap(),
            refresh_token: raw_refresh_token,
            access_token_expiry: exp,
            refresh_token_expiry: refresh_token.expiry,
        })
    }

    pub fn refresh_token(&self, refresh_token: &str) -> Result<GeneratedTokens, IggyError> {
        let now = IggyTimestamp::now().to_secs();
        if refresh_token.is_empty() {
            return Err(IggyError::InvalidRefreshToken);
        }

        let token_hash = RefreshToken::hash_token(refresh_token);
        let refresh_token = self.tokens_storage.load_refresh_token(&token_hash);
        if refresh_token.is_err() {
            return Err(IggyError::InvalidRefreshToken);
        }

        let refresh_token = refresh_token.unwrap();
        self.tokens_storage.delete_refresh_token(&token_hash)?;
        if refresh_token.expiry < now {
            return Err(IggyError::RefreshTokenExpired);
        }

        self.generate(refresh_token.user_id)
    }

    pub fn decode(
        &self,
        token: &str,
        algorithm: Algorithm,
    ) -> Result<TokenData<JwtClaims>, IggyError> {
        let validation = self.validations.get(&algorithm);
        if validation.is_none() {
            return Err(IggyError::InvalidJwtAlgorithm(
                Self::map_algorithm_to_string(algorithm),
            ));
        }

        let validation = validation.unwrap();
        match jsonwebtoken::decode::<JwtClaims>(token, &self.validator.key, validation) {
            Ok(claims) => Ok(claims),
            _ => Err(IggyError::Unauthenticated),
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

    pub async fn revoke_token(&self, token_id: &str, expiry: u64) -> Result<(), IggyError> {
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
