use server::archiver::s3::S3Archiver;
use server::archiver::Archiver;
use server::configs::server::S3ArchiverConfig;

#[tokio::test]
async fn should_not_be_initialized_given_invalid_configuration() {
    let config = S3ArchiverConfig {
        key_id: "test".to_owned(),
        key_secret: "secret".to_owned(),
        bucket: "iggy".to_owned(),
        endpoint: Some("https://iggy.s3.com".to_owned()),
        region: None,
        tmp_upload_dir: "tmp".to_owned(),
    };
    let archiver = S3Archiver::new(config);
    assert!(archiver.is_ok());
    let archiver = archiver.unwrap();
    let init = archiver.init().await;
    assert!(init.is_err());
}
