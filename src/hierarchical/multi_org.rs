//! Multi-Organization Architecture for HieraChain Framework
//!
//! This module implements the multi-organization architecture with MSP integration,
//! designed for enterprise applications. Provides support for multiple organizations,
//! affiliation hierarchies, and channel management across organizational boundaries.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::hierarchical::channel::Organization as ChannelOrg;

// ==================== Custom Errors ====================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiOrgError {
    InvalidIdentity(String),
    OrganizationNotFound(String),
    MemberNotFound(String),
    AffiliationNotFound(String),
    ChannelAlreadyExists(String),
    NetworkError(String),
}

impl std::fmt::Display for MultiOrgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiOrgError::InvalidIdentity(msg) => write!(f, "Invalid identity: {}", msg),
            MultiOrgError::OrganizationNotFound(id) => write!(f, "Organization not found: {}", id),
            MultiOrgError::MemberNotFound(id) => write!(f, "Member not found: {}", id),
            MultiOrgError::AffiliationNotFound(path) => {
                write!(f, "Affiliation not found: {}", path)
            }
            MultiOrgError::ChannelAlreadyExists(id) => write!(f, "Channel already exists: {}", id),
            MultiOrgError::NetworkError(msg) => write!(f, "Network error: {}", msg),
        }
    }
}

impl std::error::Error for MultiOrgError {}

// ==================== MSP & Policy ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchicalMSP {
    pub org_id: String,
    pub ca_cert: String,
    pub tls_ca_cert: String,
    pub admin_certs: Vec<String>,
}

impl HierarchicalMSP {
    pub fn new(
        org_id: String,
        ca_cert: String,
        tls_ca_cert: String,
        admin_certs: Vec<String>,
    ) -> Self {
        Self {
            org_id,
            ca_cert,
            tls_ca_cert,
            admin_certs,
        }
    }

    pub fn validate_identity(&self, identity: &Map<String, Value>) -> bool {
        // Simplified validation for enterprise use
        let required_fields = ["user_id", "org_id", "role"];

        for field in required_fields {
            if !identity.contains_key(field) {
                return false;
            }
        }

        // Verify organization matches
        if let Some(org_id_val) = identity.get("org_id").and_then(|v| v.as_str()) {
            if org_id_val != self.org_id {
                return false;
            }
        } else {
            return false;
        }

        // In a real implementation, this would verify certificates
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationPolicy {
    pub org_id: String,
    pub admin_threshold: usize,
    pub voting_policy: String, // "majority", "unanimous", "admin_only"
}

impl OrganizationPolicy {
    pub fn new(org_id: String, admin_threshold: usize, voting_policy: String) -> Self {
        Self {
            org_id,
            admin_threshold,
            voting_policy,
        }
    }

    pub fn evaluate_proposal(
        &self,
        votes: &HashMap<String, bool>,
        voter_roles: &HashMap<String, String>,
    ) -> bool {
        let admin_votes: HashMap<&String, &bool> = votes
            .iter()
            .filter(|(user_id, _)| voter_roles.get(*user_id).map(|r| r.as_str()) == Some("admin"))
            .collect();

        match self.voting_policy.as_str() {
            "admin_only" => {
                if admin_votes.len() < self.admin_threshold {
                    return false;
                }
                admin_votes.values().all(|&v| *v)
            }
            "unanimous" => {
                if votes.is_empty() {
                    return false;
                }
                votes.values().all(|v| *v)
            }
            _ => {
                // majority
                if votes.is_empty() {
                    return false;
                }
                let positive_votes = votes.values().filter(|v| **v).count();
                positive_votes > votes.len() / 2
            }
        }
    }
}

// ==================== Network Organization ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffiliationNode {
    pub members: Vec<String>,
    pub sub_affiliations: HashMap<String, AffiliationNode>,
    pub created_at: f64,
}

impl AffiliationNode {
    fn new() -> Self {
        Self {
            members: Vec::new(),
            sub_affiliations: HashMap::new(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub identity: Map<String, Value>,
    pub role: String,
    pub affiliation: Option<String>,
    pub registered_at: f64,
}

/// Enterprise organization with MSP integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkOrganization {
    pub org_id: String,
    pub msp: HierarchicalMSP,
    pub members: HashMap<String, MemberInfo>,
    pub channels: HashMap<String, Value>, // Storing minimal channel info reference
    pub affiliations: HashMap<String, AffiliationNode>,
}

impl NetworkOrganization {
    pub fn new(org_id: String, msp_config: &Map<String, Value>) -> Self {
        let ca_cert = msp_config
            .get("ca_cert")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let tls_ca_cert = msp_config
            .get("tls_ca_cert")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let admin_certs = msp_config
            .get("admin_certs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let msp = HierarchicalMSP::new(org_id.clone(), ca_cert, tls_ca_cert, admin_certs);

        Self {
            org_id,
            msp,
            members: HashMap::new(),
            channels: HashMap::new(),
            affiliations: HashMap::new(),
        }
    }

    pub fn register_member(
        &mut self,
        member_id: String,
        identity: Map<String, Value>,
        role: String,
    ) -> Result<String, MultiOrgError> {
        if !self.msp.validate_identity(&identity) {
            return Err(MultiOrgError::InvalidIdentity(
                "Invalid identity credentials".to_string(),
            ));
        }

        let info = MemberInfo {
            identity,
            role,
            affiliation: None,
            registered_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        };

        self.members.insert(member_id.clone(), info);
        Ok(member_id)
    }

    pub fn create_affiliation(&mut self, affiliation_path: &str) -> String {
        let parts: Vec<&str> = affiliation_path.split('.').collect();
        let mut current = &mut self.affiliations;

        for part in parts {
            current = &mut current
                .entry(part.to_string())
                .or_insert_with(AffiliationNode::new)
                .sub_affiliations;
        }

        affiliation_path.to_string()
    }

    pub fn assign_affiliation(
        &mut self,
        member_id: &str,
        affiliation_path: &str,
    ) -> Result<(), MultiOrgError> {
        if !self.members.contains_key(member_id) {
            return Err(MultiOrgError::MemberNotFound(member_id.to_string()));
        }

        // Validate affiliation path exists
        let parts: Vec<&str> = affiliation_path.split('.').collect();
        let mut current = &self.affiliations;
        for part in &parts {
            if let Some(node) = current.get(*part) {
                current = &node.sub_affiliations;
            } else {
                return Err(MultiOrgError::AffiliationNotFound(
                    affiliation_path.to_string(),
                ));
            }
        }

        // Remove from old affiliation if any
        let old_affiliation = self.members.get(member_id).unwrap().affiliation.clone();
        if let Some(old_path) = old_affiliation {
            self._remove_from_affiliation(member_id, &old_path);
        }

        // Update member info
        if let Some(member) = self.members.get_mut(member_id) {
            member.affiliation = Some(affiliation_path.to_string());
        }

        // Add to new affiliation
        let mut current_node_mut = &mut self.affiliations;
        for part in &parts {
            let node = current_node_mut.get_mut(*part).unwrap();
            if *part == *parts.last().unwrap() {
                node.members.push(member_id.to_string());
            }
            current_node_mut = &mut node.sub_affiliations;
        }

        Ok(())
    }

    fn _remove_from_affiliation(&mut self, member_id: &str, affiliation_path: &str) {
        let parts: Vec<&str> = affiliation_path.split('.').collect();
        let mut current = &mut self.affiliations;

        for part in parts {
            if let Some(node) = current.get_mut(part) {
                if let Some(pos) = node.members.iter().position(|m| m == member_id) {
                    node.members.remove(pos);
                }
                current = &mut node.sub_affiliations;
            } else {
                break;
            }
        }
    }

    pub fn get_admins(&self) -> Vec<String> {
        self.members
            .iter()
            .filter(|(_, info)| info.role == "admin")
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn get_org_policy(&self) -> OrganizationPolicy {
        let admin_count = self.get_admins().len();
        OrganizationPolicy::new(
            self.org_id.clone(),
            std::cmp::max(1, admin_count / 2 + 1),
            "majority".to_string(),
        )
    }

    pub fn get_members_by_role(&self, role: &str) -> Vec<String> {
        self.members
            .iter()
            .filter(|(_, info)| info.role == role)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn to_channel_org(&self) -> ChannelOrg {
        let admin_users = self.get_admins();
        let mut roles = HashSet::new();
        // naive role mapping
        roles.insert("member".to_string());
        if !admin_users.is_empty() {
            roles.insert("admin".to_string());
        }

        let mut org = ChannelOrg::new(
            self.org_id.clone(),
            self.org_id.clone(), // name same as id for now
            format!("{}-MSP", self.org_id),
            Vec::new(),
            HashMap::new(),
            roles,
        );
        org.admin_users = admin_users;
        org
    }
}

// ==================== Application Channel ====================

/// Application channel for multi-organization collaboration
/// Note: This wraps the core Channel but adds MultiOrg context
pub struct ApplicationChannel {
    pub channel_id: String,
    pub organizations: HashMap<String, NetworkOrganization>,
    pub config: Map<String, Value>,
    pub created_at: f64,
    // Typically would hold underlying Channel struct too
}

impl ApplicationChannel {
    pub fn new(
        channel_id: String,
        organizations: Vec<NetworkOrganization>,
        config: Map<String, Value>,
    ) -> Self {
        let mut org_map = HashMap::new();
        for org in organizations {
            org_map.insert(org.org_id.clone(), org);
        }

        Self {
            channel_id,
            organizations: org_map,
            config,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }

    pub fn add_organization(&mut self, organization: NetworkOrganization) -> bool {
        if self.organizations.contains_key(&organization.org_id) {
            return false;
        }
        self.organizations
            .insert(organization.org_id.clone(), organization);
        true
    }

    pub fn remove_organization(&mut self, org_id: &str) -> bool {
        self.organizations.remove(org_id).is_some()
    }

    pub fn validate_member_access(&self, member_id: &str, org_id: &str) -> bool {
        if let Some(org) = self.organizations.get(org_id) {
            return org.members.contains_key(member_id);
        }
        false
    }
}

// ==================== MultiOrg Network ====================

pub struct MultiOrgNetwork {
    pub organizations: HashMap<String, NetworkOrganization>,
    pub system_channel: Option<ApplicationChannel>,
    pub application_channels: HashMap<String, ApplicationChannel>,
}

impl MultiOrgNetwork {
    pub fn new() -> Self {
        Self {
            organizations: HashMap::new(),
            system_channel: None,
            application_channels: HashMap::new(),
        }
    }

    pub fn add_organization(&mut self, organization: NetworkOrganization) {
        self.organizations
            .insert(organization.org_id.clone(), organization);
    }

    pub fn remove_organization(&mut self, org_id: &str) -> bool {
        if !self.organizations.contains_key(org_id) {
            return false;
        }

        // Remove from all channels
        for channel in self.application_channels.values_mut() {
            channel.remove_organization(org_id);
        }

        self.organizations.remove(org_id);
        true
    }

    pub fn create_system_channel(
        &mut self,
        config: Map<String, Value>,
    ) -> Result<(), MultiOrgError> {
        if self.system_channel.is_some() {
            return Err(MultiOrgError::NetworkError(
                "System channel already exists".to_string(),
            ));
        }

        let all_orgs: Vec<NetworkOrganization> = self.organizations.values().cloned().collect();
        self.system_channel = Some(ApplicationChannel::new(
            "system-channel".to_string(),
            all_orgs,
            config,
        ));
        Ok(())
    }

    pub fn create_application_channel(
        &mut self,
        channel_id: String,
        participating_orgs: Vec<String>,
        config: Map<String, Value>,
    ) -> Result<(), MultiOrgError> {
        // Validate orgs
        for org_id in &participating_orgs {
            if !self.organizations.contains_key(org_id) {
                return Err(MultiOrgError::OrganizationNotFound(org_id.clone()));
            }
        }

        if self.application_channels.contains_key(&channel_id) {
            return Err(MultiOrgError::ChannelAlreadyExists(channel_id));
        }

        let orgs: Vec<NetworkOrganization> = participating_orgs
            .iter()
            .map(|id| self.organizations.get(id).unwrap().clone())
            .collect();

        let channel = ApplicationChannel::new(channel_id.clone(), orgs, config);
        self.application_channels.insert(channel_id, channel);
        Ok(())
    }

    pub fn get_channel(&self, channel_id: &str) -> Option<&ApplicationChannel> {
        if channel_id == "system-channel" {
            return self.system_channel.as_ref();
        }
        self.application_channels.get(channel_id)
    }

    pub fn get_organization(&self, org_id: &str) -> Option<&NetworkOrganization> {
        self.organizations.get(org_id)
    }
}
