use crate::state::StateSetup;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::{CREATE_STREAM_CODE, CREATE_USER_CODE};
use iggy::streams::create_stream::CreateStream;
use iggy::users::create_user::CreateUser;
use server::state::State;

#[tokio::test]
async fn should_be_empty_given_initialized_state() {
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
    let payload = command.to_bytes();
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
    assert!(entry.timestamp.as_micros() > 0);
    assert_eq!(entry.user_id, user_id);
    assert_eq!(entry.code, code);
    assert_eq!(entry.payload, payload);
    assert_eq!(entry.context, context);

    let loaded_command = CreateUser::from_bytes(entry.payload).unwrap();
    assert_eq!(command, loaded_command);
}

#[tokio::test]
async fn should_apply_multiple_entries() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    let entries = state.init().await.unwrap();

    assert!(entries.is_empty());
    assert_eq!(state.current_index(), 0);
    assert_eq!(state.entries_count(), 0);
    assert_eq!(state.term(), 0);

    let context = "test".as_bytes();
    let first_user_id = 1;
    let create_user_code = CREATE_USER_CODE;
    let create_user = CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    };
    let create_user_payload = create_user.to_bytes();
    state
        .apply(
            create_user_code,
            first_user_id,
            &create_user_payload,
            Some(context),
        )
        .await
        .unwrap();

    assert_eq!(state.current_index(), 0);
    assert_eq!(state.entries_count(), 1);

    let create_user_context = "test".as_bytes();
    let second_user_id = 2;
    let create_stream_code = CREATE_STREAM_CODE;
    let create_stream = CreateStream {
        stream_id: Some(1),
        name: "test".to_string(),
    };
    let create_stream_payload = create_stream.to_bytes();
    state
        .apply(
            create_stream_code,
            second_user_id,
            &create_stream_payload,
            None,
        )
        .await
        .unwrap();

    assert_eq!(state.current_index(), 1);
    assert_eq!(state.entries_count(), 2);

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 2);

    let create_user_entry = entries.remove(0);
    assert_eq!(create_user_entry.index, 0);
    assert_eq!(create_user_entry.term, 0);
    assert_eq!(create_user_entry.version, setup.version());
    assert_eq!(create_user_entry.flags, 0);
    assert!(create_user_entry.timestamp.as_micros() > 0);
    assert_eq!(create_user_entry.user_id, 1);
    assert_eq!(create_user_entry.code, create_user_code);
    assert_eq!(create_user_entry.payload, create_user_payload);
    assert_eq!(create_user_entry.context, create_user_context);

    let loaded_create_user = CreateUser::from_bytes(create_user_entry.payload).unwrap();
    assert_eq!(create_user, loaded_create_user);

    let create_stream_entry = entries.remove(0);
    assert_eq!(create_stream_entry.index, 1);
    assert_eq!(create_stream_entry.term, 0);
    assert_eq!(create_stream_entry.version, setup.version());
    assert_eq!(create_stream_entry.flags, 0);
    assert!(create_stream_entry.timestamp.as_micros() > 0);
    assert!(create_stream_entry.timestamp.as_micros() > create_user_entry.timestamp.as_micros());
    assert_eq!(create_stream_entry.user_id, 2);
    assert_eq!(create_stream_entry.code, create_stream_code);
    assert_eq!(create_stream_entry.payload, create_stream_payload);
    assert!(create_stream_entry.context.is_empty());

    let loaded_create_stream = CreateStream::from_bytes(create_stream_entry.payload).unwrap();
    assert_eq!(create_stream, loaded_create_stream);
}
