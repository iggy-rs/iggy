use crate::streaming::common::test_setup::TestSetup;
use iggy::snapshot::{SnapshotCompression, SystemSnapshotType};
use server::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
use server::streaming::session::Session;
use server::streaming::systems::system::System;
use std::io::{Cursor, Read};
use std::net::{Ipv4Addr, SocketAddr};
use zip::ZipArchive;

#[tokio::test]
async fn should_create_snapshot_file() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    )
    .await;

    system.init().await.unwrap();

    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));

    let snapshot = system
        .get_snapshot(
            &session,
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::Test],
        )
        .await
        .unwrap();
    assert!(!snapshot.0.is_empty());

    let cursor = Cursor::new(snapshot.0);
    let mut zip = ZipArchive::new(cursor).unwrap();
    let mut test_file = zip.by_name("test.txt").unwrap();
    let mut test_content = String::new();
    test_file.read_to_string(&mut test_content).unwrap();
    assert_eq!(test_content, "test\n");
}
