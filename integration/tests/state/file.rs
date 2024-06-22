use crate::state::StateSetup;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::CREATE_USER_CODE;
use iggy::users::create_user::CreateUser;
use server::state::State;

#[tokio::test]
async fn should_be_empty_given_default_state() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();
    let entries = state.load_entries().await.unwrap();
    assert!(entries.is_empty());
}

#[tokio::test]
async fn should_apply_single_entry() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();

    let context = "test".as_bytes();
    let user_id = 1;
    let code = CREATE_USER_CODE;
    let command = CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    };
    let payload = command.as_bytes();
    state
        .apply(code, user_id, &payload, Some(context))
        .await
        .unwrap();

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    assert_eq!(entry.index, 0);
    assert_eq!(entry.term, 0);
    assert_eq!(entry.version, setup.version());
    assert_eq!(entry.flags, 0);
    assert!(entry.timestamp.to_micros() > 0);
    assert_eq!(entry.user_id, user_id);
    assert_eq!(entry.code, code);
    assert_eq!(entry.payload, payload);
    assert_eq!(entry.context, context);

    let loaded_command = CreateUser::from_bytes(entry.payload).unwrap();
    assert_eq!(command, loaded_command);
}
