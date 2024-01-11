use iggy::models::permissions::Permissions;

#[derive(Debug, Clone, Default)]
pub(crate) struct PermissionsTestArgs {
    pub(crate) global_permissions: Option<String>,
    pub(crate) stream_permissions: Vec<String>,
    pub(crate) expected_permissions: Option<Permissions>,
}

impl PermissionsTestArgs {
    pub(crate) fn new(
        global_permissions: Option<String>,
        stream_permissions: Vec<String>,
        expected_permissions: Option<Permissions>,
    ) -> Self {
        Self {
            global_permissions,
            stream_permissions,
            expected_permissions,
        }
    }

    pub(crate) fn as_arg(&self) -> Vec<String> {
        let mut args = vec![];
        if let Some(global_permissions) = &self.global_permissions {
            args.push(String::from("--global-permissions"));
            args.push(global_permissions.clone());
        }

        args.extend(
            self.stream_permissions
                .iter()
                .flat_map(|i| vec![String::from("--stream-permissions"), i.clone()])
                .collect::<Vec<String>>(),
        );
        args
    }
}
