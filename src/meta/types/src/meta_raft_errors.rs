// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use openraft::error::ChangeMembershipError;
pub use openraft::error::ClientWriteError;
pub use openraft::error::EmptyMembership;
pub use openraft::error::Fatal;
pub use openraft::error::ForwardToLeader;
pub use openraft::error::InProgress;
pub use openraft::error::InitializeError;
pub use openraft::error::LearnerIsLagging;
pub use openraft::error::LearnerNotFound;
use openraft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::MetaError;

/// Raft protocol related errors
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MetaRaftError {
    #[error(transparent)]
    Fatal(#[from] Fatal),

    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error("{0}")]
    ConsistentReadError(String),

    #[error("{0}")]
    ForwardRequestError(String),

    #[error("{0}")]
    NoLeaderError(String),

    #[error("{0}")]
    RequestNotForwardToLeaderError(String),

    #[error(transparent)]
    InitializeError(InitializeError),
}

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

/// Collection of errors that occur when writing a raft-log to local raft node.
/// This does not include the errors raised when writing a membership log.
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RaftWriteError {
    #[error(transparent)]
    Fatal(#[from] Fatal),

    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),
}

impl RaftWriteError {
    pub fn from_raft_err(e: ClientWriteError) -> Self {
        match e {
            ClientWriteError::Fatal(fatal) => fatal.into(),
            ClientWriteError::ForwardToLeader(to_leader) => to_leader.into(),
            ClientWriteError::ChangeMembershipError(_) => {
                unreachable!("there should not be a ChangeMembershipError for client_write")
            }
        }
    }
}

impl From<RaftWriteError> for MetaRaftError {
    fn from(e: RaftWriteError) -> Self {
        match e {
            RaftWriteError::ForwardToLeader(e) => e.into(),
            RaftWriteError::Fatal(e) => e.into(),
        }
    }
}

impl From<RaftWriteError> for MetaError {
    fn from(e: RaftWriteError) -> Self {
        let re = MetaRaftError::from(e);
        MetaError::MetaRaftError(re)
    }
}

/// RaftChangeMembershipError is a super set of RaftWriteError.
impl From<RaftWriteError> for RaftChangeMembershipError {
    fn from(e: RaftWriteError) -> Self {
        match e {
            RaftWriteError::Fatal(fatal) => fatal.into(),
            RaftWriteError::ForwardToLeader(to_leader) => to_leader.into(),
        }
    }
}

// Collection of errors that occur when change membership on local raft node.
pub type RaftChangeMembershipError = ClientWriteError;

impl From<RaftChangeMembershipError> for MetaRaftError {
    fn from(e: RaftChangeMembershipError) -> Self {
        match e {
            RaftChangeMembershipError::ForwardToLeader(e) => e.into(),
            RaftChangeMembershipError::ChangeMembershipError(e) => e.into(),
            RaftChangeMembershipError::Fatal(e) => e.into(),
        }
    }
}

impl From<RaftChangeMembershipError> for MetaError {
    fn from(e: RaftChangeMembershipError) -> Self {
        let re = MetaRaftError::from(e);
        MetaError::MetaRaftError(re)
    }
}
