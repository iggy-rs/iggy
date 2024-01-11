use crate::streaming::common::test_setup::TestSetup;
use iggy::utils::timestamp::IggyTimestamp;
use server::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;

#[tokio::test]
async fn many_personal_access_tokens_should_be_saved_and_loaded() {
    let setup = TestSetup::init().await;
    let now = IggyTimestamp::now().to_micros();
    let (pat1, raw_token1) = PersonalAccessToken::new(1, "test1", now, None);
    let (pat2, raw_token2) = PersonalAccessToken::new(2, "test2", now, Some(1000));
    let (pat3, raw_token3) = PersonalAccessToken::new(3, "test3", now, Some(100_000));

    setup
        .storage
        .personal_access_token
        .save(&pat1)
        .await
        .expect("Failed to save personal access token");
    setup
        .storage
        .personal_access_token
        .save(&pat2)
        .await
        .expect("Failed to save personal access token");
    setup
        .storage
        .personal_access_token
        .save(&pat3)
        .await
        .expect("Failed to save personal access token");

    let personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_all()
        .await
        .expect("Failed to load personal access tokens");
    assert_eq!(personal_access_tokens.len(), 3);

    let user1_personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_for_user(pat1.user_id)
        .await
        .expect("Failed to load personal access tokens");
    assert_eq!(user1_personal_access_tokens.len(), 1);
    assert_eq!(user1_personal_access_tokens[0], pat1);
    let user2_personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_for_user(pat2.user_id)
        .await
        .expect("Failed to load personal access tokens");
    assert_eq!(user2_personal_access_tokens.len(), 1);
    assert_eq!(user2_personal_access_tokens[0], pat2);
    let user3_personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_for_user(pat3.user_id)
        .await
        .expect("Failed to load personal access tokens");
    assert_eq!(user3_personal_access_tokens.len(), 1);
    assert_eq!(user3_personal_access_tokens[0], pat3);

    let loaded_pat1 = setup
        .storage
        .personal_access_token
        .load_by_token(&pat1.token)
        .await
        .expect("Failed to load personal access token");
    let loaded_pat2 = setup
        .storage
        .personal_access_token
        .load_by_token(&pat2.token)
        .await
        .expect("Failed to load personal access token");
    let loaded_pat3 = setup
        .storage
        .personal_access_token
        .load_by_token(&pat3.token)
        .await
        .expect("Failed to load personal access token");

    assert_pat(&pat1, &loaded_pat1);
    assert_pat(&pat2, &loaded_pat2);
    assert_pat(&pat3, &loaded_pat3);

    assert_ne!(loaded_pat1.token, raw_token1);
    assert_ne!(loaded_pat2.token, raw_token2);
    assert_ne!(loaded_pat3.token, raw_token3);
}

fn assert_pat(personal_access_token: &PersonalAccessToken, loaded_pat: &PersonalAccessToken) {
    assert_eq!(loaded_pat.user_id, personal_access_token.user_id);
    assert_eq!(loaded_pat.name, personal_access_token.name);
    assert_eq!(loaded_pat.token, personal_access_token.token);
    assert_eq!(loaded_pat.expiry, personal_access_token.expiry);
}

#[tokio::test]
async fn personal_access_token_should_be_deleted() {
    let setup = TestSetup::init().await;
    let user_id = 1;
    let now = IggyTimestamp::now().to_micros();
    let (personal_access_token, _) = PersonalAccessToken::new(user_id, "test", now, None);
    setup
        .storage
        .personal_access_token
        .save(&personal_access_token)
        .await
        .expect("Failed to save personal access token");

    let personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_all()
        .await
        .expect("Failed to load personal access tokens");
    assert_eq!(personal_access_tokens.len(), 1);
    let loaded_pat = setup
        .storage
        .personal_access_token
        .load_by_token(&personal_access_token.token)
        .await
        .expect("Failed to load personal access token");
    assert_pat(&personal_access_token, &loaded_pat);

    let loaded_pat_by_name = setup
        .storage
        .personal_access_token
        .load_by_name(user_id, &personal_access_token.name)
        .await
        .expect("Failed to load personal access token");
    assert_pat(&personal_access_token, &loaded_pat_by_name);

    setup
        .storage
        .personal_access_token
        .delete_for_user(personal_access_token.user_id, &personal_access_token.name)
        .await
        .expect("Failed to delete personal access token");

    let loaded_pat = setup
        .storage
        .personal_access_token
        .load_by_token(&personal_access_token.token)
        .await;
    assert!(loaded_pat.is_err());

    let loaded_pat_by_name = setup
        .storage
        .personal_access_token
        .load_by_name(user_id, &personal_access_token.name)
        .await;
    assert!(loaded_pat_by_name.is_err());

    let personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_all()
        .await
        .expect("Failed to load personal access tokens");
    assert!(personal_access_tokens.is_empty());

    let user_personal_access_tokens = setup
        .storage
        .personal_access_token
        .load_for_user(user_id)
        .await
        .expect("Failed to load personal access tokens");
    assert!(user_personal_access_tokens.is_empty());
}
