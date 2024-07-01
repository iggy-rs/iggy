use crate::state::StateSetup;
use iggy::streams::create_stream::CreateStream;
use iggy::users::create_user::CreateUser;
use server::state::command::EntryCommand;
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

    let user_id = 1;
    let command = EntryCommand::CreateUser(CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    });
    let command_clone = EntryCommand::CreateUser(CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    });

    state.apply(user_id, command).await.unwrap();

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    assert_eq!(entry.index, 0);
    assert_eq!(entry.term, 0);
    assert_eq!(entry.version, setup.version());
    assert_eq!(entry.flags, 0);
    assert!(entry.timestamp.as_micros() > 0);
    assert_eq!(entry.user_id, user_id);
    assert_eq!(entry.command, command_clone);
    assert!(entry.context.is_empty());
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

    let first_user_id = 1;
    let create_user = EntryCommand::CreateUser(CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    });
    let create_user_clone = EntryCommand::CreateUser(CreateUser {
        username: "test".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    });

    state.apply(first_user_id, create_user).await.unwrap();

    assert_eq!(state.current_index(), 0);
    assert_eq!(state.entries_count(), 1);

    let second_user_id = 2;
    let create_stream = EntryCommand::CreateStream(CreateStream {
        stream_id: Some(1),
        name: "test".to_string(),
    });
    let create_stream_clone = EntryCommand::CreateStream(CreateStream {
        stream_id: Some(1),
        name: "test".to_string(),
    });

    state.apply(second_user_id, create_stream).await.unwrap();

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
    assert!(create_user_entry.context.is_empty());
    assert_eq!(create_user_entry.command, create_user_clone);

    let create_stream_entry = entries.remove(0);
    assert_eq!(create_stream_entry.index, 1);
    assert_eq!(create_stream_entry.term, 0);
    assert_eq!(create_stream_entry.version, setup.version());
    assert_eq!(create_stream_entry.flags, 0);
    assert!(create_stream_entry.timestamp.as_micros() > 0);
    assert!(create_stream_entry.timestamp.as_micros() > create_user_entry.timestamp.as_micros());
    assert_eq!(create_stream_entry.user_id, 2);
    assert!(create_stream_entry.context.is_empty());
    assert_eq!(create_stream_clone, create_stream_entry.command);
}
