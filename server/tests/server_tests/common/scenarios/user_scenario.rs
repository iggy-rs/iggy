use crate::server_tests::common::{ClientFactory, TestServer};
use iggy::client::UserClient;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::identifier::Identifier;
use iggy::models::permissions::{GlobalPermissions, Permissions};
use iggy::models::user_status::UserStatus;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;

const ROOT_USERNAME: &str = "iggy";
const ROOT_PASSWORD: &str = "iggy";

pub async fn run(client_factory: &dyn ClientFactory) {
    let mut test_server = TestServer::default();
    test_server.enable_authentication();
    test_server.enable_authorization();
    test_server.start();
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, IggyClientConfig::default(), None, None, None);

    let identity_info = client
        .login_user(&LoginUser {
            username: ROOT_USERNAME.to_string(),
            password: ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 1);

    let users = client.get_users(&GetUsers {}).await.unwrap();
    assert_eq!(users.len(), 1);

    let user = users.get(0).unwrap();
    assert_eq!(user.id, 1);
    assert!(user.created_at > 0);
    assert_eq!(user.username, ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);

    let user = client
        .get_user(&GetUser {
            user_id: Identifier::named(ROOT_USERNAME).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.created_at > 0);
    assert_eq!(user.username, ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);
    assert!(user.permissions.is_some());

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

    let identity_info = client
        .login_user(&LoginUser {
            username: test_user.to_string(),
            password: test_password.to_string(),
        })
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 2);

    let create_user_result = client
        .create_user(&CreateUser {
            username: "user2".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: None,
        })
        .await;

    assert!(create_user_result.is_err());

    client
        .login_user(&LoginUser {
            username: ROOT_USERNAME.to_string(),
            password: ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();

    let create_duplicated_user = client
        .create_user(&CreateUser {
            username: test_user.to_string(),
            password: test_password.to_string(),
            status: UserStatus::Active,
            permissions: None,
        })
        .await;

    assert!(create_duplicated_user.is_err());

    let updated_test_user = "user2";

    client
        .update_user(&UpdateUser {
            user_id: Identifier::named(test_user).unwrap(),
            username: Some(updated_test_user.to_string()),
            status: Some(UserStatus::Inactive),
        })
        .await
        .unwrap();

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

    let updated_test_password = "topsecret";

    client
        .change_password(&ChangePassword {
            user_id: Identifier::named(updated_test_user).unwrap(),
            current_password: test_password.to_string(),
            new_password: updated_test_password.to_string(),
        })
        .await
        .unwrap();

    let change_password = client
        .change_password(&ChangePassword {
            user_id: Identifier::named(updated_test_user).unwrap(),
            current_password: test_password.to_string(),
            new_password: updated_test_password.to_string(),
        })
        .await;

    assert!(change_password.is_err());

    client
        .delete_user(&DeleteUser {
            user_id: Identifier::named(updated_test_user).unwrap(),
        })
        .await
        .unwrap();

    let delete_root_user = client
        .delete_user(&DeleteUser {
            user_id: Identifier::named(ROOT_USERNAME).unwrap(),
        })
        .await;

    assert!(delete_root_user.is_err());

    client.logout_user(&LogoutUser {}).await.unwrap();

    let get_users = client.get_users(&GetUsers {}).await;
    assert!(get_users.is_err());

    test_server.stop();
}
