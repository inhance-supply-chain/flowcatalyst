//! Local RBAC catalogue — declarative role→permission map evaluated inside
//! the consumer app. Mirrors `@flowcatalyst/sdk/fastify`'s `defineRbac`.
//!
//! Permissions live in the app, not on the token: FlowCatalyst tokens carry
//! roles only. The catalogue maps those roles to whatever capability
//! vocabulary the app cares about, evaluated locally without a round-trip.
//!
//! ```ignore
//! use fc_sdk::auth::axum::RbacBuilder;
//! let rbac = RbacBuilder::new()
//!     .role("billing-admin").grants(["invoice:create", "invoice:read"])
//!     .role("support").grants(["ticket:*"])
//!     .build();
//! ```
//!
//! Wildcards are `:`-segment suffixes only — `ticket:*` matches `ticket:read`
//! and `ticket:foo:bar`; `*` matches everything. Mid-segment globs are not
//! supported.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Frozen, share-by-`Arc` view of the catalogue. Built from [`RbacBuilder`].
#[derive(Debug, Clone)]
pub struct RbacCatalogue {
    inner: Arc<RbacInner>,
}

#[derive(Debug)]
struct RbacInner {
    roles: HashMap<String, HashSet<String>>,
}

impl RbacCatalogue {
    /// Union of permissions across the given roles. Unknown roles are
    /// silently ignored.
    pub fn resolve(&self, role_names: &[String]) -> Vec<String> {
        let mut out: HashSet<String> = HashSet::new();
        for role in role_names {
            if let Some(perms) = self.inner.roles.get(role) {
                for p in perms {
                    out.insert(p.clone());
                }
            }
        }
        out.into_iter().collect()
    }

    /// Wildcard-aware membership: `permission_set` may contain entries like
    /// `"ticket:*"` or `"*"`; `needed` is the literal permission being asked
    /// for. Wildcards are suffix-only on `:` segment boundaries.
    pub(crate) fn matches_any(permission_set: &HashSet<String>, needed: &str) -> bool {
        if permission_set.contains(needed) || permission_set.contains("*") {
            return true;
        }
        let segments: Vec<&str> = needed.split(':').collect();
        // Try progressively shorter prefixes: "a:b:c" → "a:b:*", "a:*"
        for i in (1..segments.len()).rev() {
            let prefix = segments[..i].join(":");
            if permission_set.contains(&format!("{prefix}:*")) {
                return true;
            }
        }
        false
    }
}

/// Builder. Use [`RbacBuilder::new`] then chain `.role(...).grants(...)`.
#[derive(Debug, Default)]
pub struct RbacBuilder {
    roles: HashMap<String, HashSet<String>>,
}

impl RbacBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin defining grants for a role. Returns a scope object; call
    /// `.grants(...)` to attach permissions.
    pub fn role(self, name: impl Into<String>) -> RoleScope {
        let name = name.into();
        assert!(!name.is_empty(), "RBAC role name cannot be empty");
        RoleScope {
            builder: self,
            role: name,
        }
    }

    pub fn build(self) -> RbacCatalogue {
        RbacCatalogue {
            inner: Arc::new(RbacInner { roles: self.roles }),
        }
    }
}

/// Returned by [`RbacBuilder::role`]. Attach permissions, then either chain
/// another `.role(...)` or call `.build()`.
pub struct RoleScope {
    builder: RbacBuilder,
    role: String,
}

impl RoleScope {
    pub fn grants<I, S>(self, permissions: I) -> RbacBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let RoleScope { mut builder, role } = self;
        let entry = builder.roles.entry(role.clone()).or_default();
        for p in permissions {
            let s = p.into();
            assert!(!s.is_empty(), "RBAC permission for role {role:?} is empty");
            entry.insert(s);
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roles(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn unions_permissions_across_roles() {
        let r = RbacBuilder::new()
            .role("a")
            .grants(["p1", "p2"])
            .role("b")
            .grants(["p2", "p3"])
            .build();
        let mut got = r.resolve(&roles(&["a", "b"]));
        got.sort();
        assert_eq!(got, vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn ignores_unknown_roles() {
        let r = RbacBuilder::new().role("a").grants(["p1"]).build();
        assert_eq!(r.resolve(&roles(&["a", "ghost"])), vec!["p1".to_string()]);
    }

    #[test]
    fn wildcard_matches_at_any_segment_depth() {
        let mut set: HashSet<String> = HashSet::new();
        set.insert("billing:*".to_string());
        assert!(RbacCatalogue::matches_any(&set, "billing:read"));
        assert!(RbacCatalogue::matches_any(&set, "billing:invoice:read"));
        assert!(!RbacCatalogue::matches_any(&set, "ticket:read"));
    }

    #[test]
    fn full_wildcard_matches_everything() {
        let mut set: HashSet<String> = HashSet::new();
        set.insert("*".to_string());
        assert!(RbacCatalogue::matches_any(&set, "anything:goes:here"));
    }

    #[test]
    fn literal_match_works() {
        let mut set: HashSet<String> = HashSet::new();
        set.insert("billing:read".to_string());
        assert!(RbacCatalogue::matches_any(&set, "billing:read"));
        assert!(!RbacCatalogue::matches_any(&set, "billing:write"));
    }

    #[test]
    fn multiple_grants_on_same_role_accumulate() {
        let r = RbacBuilder::new()
            .role("a")
            .grants(["p1", "p2"])
            .role("a")
            .grants(["p3"])
            .build();
        let mut got = r.resolve(&roles(&["a"]));
        got.sort();
        assert_eq!(got, vec!["p1", "p2", "p3"]);
    }
}
