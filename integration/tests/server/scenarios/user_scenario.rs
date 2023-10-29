use iggy::client::{PersonalAccessTokenClient, SystemClient, UserClient};
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::identifier::Identifier;
use iggy::models::permissions::{GlobalPermissions, Permissions};
use iggy::models::user_status::UserStatus;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::system::ping::Ping;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::defaults::*;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use integration::test_server::{assert_clean_system, ClientFactory};

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);

    // 1. Ping should be allowed for unauthenticated users
    client.ping(&Ping {}).await.unwrap();

    // 2. Any other operation except the login should be forbidden for unauthenticated users
    let get_users = client.get_users(&GetUsers {}).await;
    assert!(get_users.is_err());

    // 3. Login as root user
    let identity_info = client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 1);

    // 4. List all users
    let users = client.get_users(&GetUsers {}).await.unwrap();
    assert_eq!(users.len(), 1);

    // 5. Get user details
    let user = users.get(0).unwrap();
    assert_eq!(user.id, 1);
    assert!(user.created_at > 0);
    assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);

    let user = client
        .get_user(&GetUser {
            user_id: Identifier::named(DEFAULT_ROOT_USERNAME).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.created_at > 0);
    assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);
    assert!(user.permissions.is_some());

    // 6. Create a new user
    let test_user = "user";
    let test_password = "secret";
    client
        .create_user(&CreateUser {
            username: test_user.to_string(),
            password: test_password.to_string(),
            status: UserStatus::Active,
            permissions: Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: true,
                    manage_users: false,
                    read_users: true,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        })
        .await
        .unwrap();

    // 7. Trying to login with the invalid username should fail
    let login_user = client
        .login_user(&LoginUser {
            username: "user2".to_string(),
            password: test_password.to_string(),
        })
        .await;

    assert!(login_user.is_err());

    // 8. Trying to login with the invalid password should fail
    let login_user = client
        .login_user(&LoginUser {
            username: test_user.to_string(),
            password: "secret123".to_string(),
        })
        .await;

    assert!(login_user.is_err());

    // 9. Login as the new user
    let identity_info = client
        .login_user(&LoginUser {
            username: test_user.to_string(),
            password: test_password.to_string(),
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 2);

    // 10. Trying to create a new user (or any other resource) without the appropriate permissions should fail
    let create_user_result = client
        .create_user(&CreateUser {
            username: "user2".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: None,
        })
        .await;

    assert!(create_user_result.is_err());

    // 11. Updating your own password should be allowed
    let updated_test_password = "topsecret";

    client
        .change_password(&ChangePassword {
            user_id: Identifier::named(test_user).unwrap(),
            current_password: test_password.to_string(),
            new_password: updated_test_password.to_string(),
        })
        .await
        .unwrap();

    // 12. Trying to change the password given the invalid current password should fail
    let change_password = client
        .change_password(&ChangePassword {
            user_id: Identifier::named(test_user).unwrap(),
            current_password: test_password.to_string(),
            new_password: updated_test_password.to_string(),
        })
        .await;

    assert!(change_password.is_err());

    let pat_name1 = "test_token_1";
    let pat_name2 = "test_token_2";

    // 13. Create the personal access tokens
    let raw_pat1 = client
        .create_personal_access_token(&CreatePersonalAccessToken {
            name: pat_name1.to_string(),
            expiry: Some(1000),
        })
        .await
        .unwrap();

    assert!(!raw_pat1.token.is_empty());

    let raw_pat2 = client
        .create_personal_access_token(&CreatePersonalAccessToken {
            name: pat_name2.to_string(),
            expiry: None,
        })
        .await
        .unwrap();

    assert!(!raw_pat2.token.is_empty());

    // 14. Get personal access tokens and verify that the token is there
    let personal_access_tokens = client
        .get_personal_access_tokens(&GetPersonalAccessTokens {})
        .await
        .unwrap();
    assert_eq!(personal_access_tokens.len(), 2);

    // 15. Logout
    client.logout_user(&LogoutUser {}).await.unwrap();

    // 16. Login with the personal access tokens
    let identity_info = client
        .login_with_personal_access_token(&LoginWithPersonalAccessToken {
            token: raw_pat1.token,
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 2);

    let identity_info = client
        .login_with_personal_access_token(&LoginWithPersonalAccessToken {
            token: raw_pat2.token,
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 2);

    // 17. Delete the personal access tokens
    client
        .delete_personal_access_token(&DeletePersonalAccessToken {
            name: pat_name1.to_string(),
        })
        .await
        .unwrap();

    client
        .delete_personal_access_token(&DeletePersonalAccessToken {
            name: pat_name2.to_string(),
        })
        .await
        .unwrap();

    // 18. Get personal access tokens and verify that the token is no longer available
    let personal_access_tokens = client
        .get_personal_access_tokens(&GetPersonalAccessTokens {})
        .await
        .unwrap();
    assert!(personal_access_tokens.is_empty());

    // 19. Login as root user again
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();

    // 20. Trying to create a new user with the same username should fail
    let create_duplicated_user = client
        .create_user(&CreateUser {
            username: test_user.to_string(),
            password: test_password.to_string(),
            status: UserStatus::Active,
            permissions: None,
        })
        .await;

    assert!(create_duplicated_user.is_err());

    // 21. Update user details
    let updated_test_user = "user2";

    client
        .update_user(&UpdateUser {
            user_id: Identifier::named(test_user).unwrap(),
            username: Some(updated_test_user.to_string()),
            status: Some(UserStatus::Inactive),
        })
        .await
        .unwrap();

    // 22. Update user permissions
    client
        .update_permissions(&UpdatePermissions {
            user_id: Identifier::named(updated_test_user).unwrap(),
            permissions: Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: true,
                    manage_users: false,
                    read_users: true,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        })
        .await
        .unwrap();

    // 23. Deleting another user should be allowed
    client
        .delete_user(&DeleteUser {
            user_id: Identifier::named(updated_test_user).unwrap(),
        })
        .await
        .unwrap();

    // 24. Trying to delete the root user should fail
    let delete_root_user = client
        .delete_user(&DeleteUser {
            user_id: Identifier::named(DEFAULT_ROOT_USERNAME).unwrap(),
        })
        .await;

    assert!(delete_root_user.is_err());

    assert_clean_system(&client).await;

    // 25. Logout
    client.logout_user(&LogoutUser {}).await.unwrap();

    // 26. Trying to perform any secured operation after logout should fail
    let get_users = client.get_users(&GetUsers {}).await;
    assert!(get_users.is_err());
}
