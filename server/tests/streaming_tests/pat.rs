use crate::streaming_tests::common::test_setup::TestSetup;
use server::streaming::users::pat::PersonalAccessToken;

#[tokio::test]
async fn many_personal_access_tokens_should_be_saved_and_loaded() {
    let setup = TestSetup::init().await;
    let (pat1, raw_token1) = PersonalAccessToken::new(None);
    let (pat2, raw_token2) = PersonalAccessToken::new(Some(1000));
    let (pat3, raw_token3) = PersonalAccessToken::new(Some(100_000));

    setup
        .storage
        .user
        .save_pat(1, &raw_token1, &pat1)
        .await
        .unwrap();
    setup
        .storage
        .user
        .save_pat(2, &raw_token2, &pat2)
        .await
        .unwrap();
    setup
        .storage
        .user
        .save_pat(3, &raw_token3, &pat3)
        .await
        .unwrap();

    let loaded_pat1 = setup.storage.user.load_pat(1, &raw_token1).await.unwrap();
    let loaded_pat2 = setup.storage.user.load_pat(2, &raw_token2).await.unwrap();
    let loaded_pat3 = setup.storage.user.load_pat(3, &raw_token3).await.unwrap();

    assert_pat(&pat1, &loaded_pat1);
    assert_pat(&pat2, &loaded_pat2);
    assert_pat(&pat3, &loaded_pat3);

    assert_ne!(loaded_pat1.token, raw_token1);
    assert_ne!(loaded_pat2.token, raw_token2);
    assert_ne!(loaded_pat3.token, raw_token3);
}

fn assert_pat(pat: &PersonalAccessToken, loaded_pat: &PersonalAccessToken) {
    assert_eq!(loaded_pat.token, pat.token);
    assert_eq!(loaded_pat.expiry, pat.expiry);
}

#[tokio::test]
async fn personal_access_token_should_be_deleted() {
    let setup = TestSetup::init().await;
    let user_id = 1;
    let (pat, raw_token) = PersonalAccessToken::new(None);
    setup
        .storage
        .user
        .save_pat(user_id, &raw_token, &pat)
        .await
        .unwrap();

    let loaded_pat = setup
        .storage
        .user
        .load_pat(user_id, &raw_token)
        .await
        .unwrap();
    assert_pat(&pat, &loaded_pat);

    setup
        .storage
        .user
        .delete_pat(user_id, &raw_token)
        .await
        .unwrap();

    let loaded_pat = setup.storage.user.load_pat(user_id, &raw_token).await;
    assert!(loaded_pat.is_err());
}
