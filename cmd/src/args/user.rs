use crate::args::permissions::stream::StreamPermissionsArg;
use crate::args::permissions::UserStatusArg;
use clap::{Args, Subcommand};
use iggy::identifier::Identifier;
use std::convert::From;

use super::permissions::global::GlobalPermissionsArg;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum UserAction {
    /// Create user with given username and password
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples
    ///  iggy user create testuser pass#1%X!
    ///  iggy user create guest guess --user-status inactive
    #[clap(verbatim_doc_comment)]
    Create(UserCreateArgs),
    /// Delete user with given ID
    ///
    /// User ID can be specified as a username or ID
    ///
    /// Examples:
    ///  iggy user delete 2
    ///  iggy user delete testuser
    #[clap(verbatim_doc_comment)]
    Delete(UserDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserCreateArgs {
    /// Username
    pub(crate) username: String,
    /// Password
    pub(crate) password: String,
    /// User status
    #[clap(short, long)]
    #[arg(value_enum, default_value_t = UserStatusArg::default())]
    pub(crate) user_status: UserStatusArg,
    /// Set global permissions for created user
    ///
    /// All global permissions by default are set to false and this command line option
    /// allows to set each permission individually. Permissions are separated
    /// by comma and each permission is identified by the same name as in the iggy
    /// SDK in iggy::models::permissions::GlobalPermissions struct. For each permission
    /// there's long variant (same as in SDK) and short variant.
    ///
    /// Available permissions (long and short versions):  manage_servers / m_srv,
    /// read_servers / r_srv, manage_users / m_usr, read_users / r_usr,
    /// manage_streams / m_str, read_streams / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg
    ///
    /// Examples:
    ///  iggy user create guest guess --global-permissions p_msg,s_msg
    ///  iggy user create admin pass#1%X! -g m_srv,r_srv,m_usr,r_usr,m_str,r_str,m_top,r_top,p_msg,s_msg
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(GlobalPermissionsArg))]
    pub(crate) global_permissions: Option<GlobalPermissionsArg>,
    /// Set stream permissions for created user
    ///
    /// Stream permissions are defined by each stream separately. Setting permission for stream
    /// allows to set each permission individually, by default, if no permission is provided
    /// (only stream ID is provided) all are set fo false. Stream permission format consists
    /// of stream ID followed by colon (:) and list of permissions separated by comma (,).
    /// For each stream permission there's long variant (same as in SDK in
    /// iggy::models::permissions::StreamPermissions) and short variant.
    ///
    /// Available stream permissions: manage_stream / m_str, read_stream / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg.
    ///
    /// For each stream one can set permissions for each topic separately. Topic permissions
    /// are defined for each topic separately. Setting permission for topic allows to set each
    /// permission individually, by default, if no permission is provided (only topic ID is provided)
    /// all are set fo false. Topic permission format consists of topic ID followed by colon (:)
    /// and list of permissions separated by comma (,). For each topic permission there's long
    /// variant (same as in SDK in iggy::models::permissions::TopicPermissions) and short variant.
    /// Topic permissions are separated by hash (#) after stream permissions.
    ///
    /// Available topic permissions: manage_topic / m_top, read_topic / r_top, poll_messages / p_msg,
    /// send_messages / s_msg.
    ///
    /// Permissions format: STREAM_ID[:STREAM_PERMISSIONS][#TOPIC_ID[:TOPIC_PERMISSIONS]]
    ///
    /// Examples:
    ///  iggy user create guest guest -s 1:manage_topics,read_topics
    ///  iggy user create admin p@Ss! --stream-permissions 2:m_str,r_str,m_top,r_top,p_msg,s_msg
    ///  iggy user create sender s3n43r -s 3#1:s_msg#2:s_msg
    ///  iggy user create user1 test12 -s 4:manage_stream,r_top#1:s_msg,p_msg#2:manage_topic
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(StreamPermissionsArg))]
    pub(crate) stream_permissions: Option<Vec<StreamPermissionsArg>>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserDeleteArgs {
    /// User ID to delete
    ///
    /// User ID can be specified as a username or ID
    pub(crate) user_id: Identifier,
}
