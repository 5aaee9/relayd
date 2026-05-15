use crate::model::{Allocation, Binding, Protocol};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ConnectionTrait, DatabaseConnection, DbBackend, DbErr,
    EntityTrait, IntoActiveModel, QueryOrder, Schema, Statement,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::collections::HashMap;
use std::num::TryFromIntError;
use std::str::FromStr;
use std::time::Duration;

mod allocation_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "allocations")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub protocol: String,
        pub port: i32,
        pub target_port: i32,
        pub host: Option<String>,
        pub created_at_ms: i64,
        pub updated_at_ms: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

mod binding_entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "bindings")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub allocation_id: String,
        pub target_port: i32,
        pub host: Option<String>,
        pub created_at_ms: i64,
        pub updated_at_ms: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub struct Repository {
    pool: SqlitePool,
    db: DatabaseConnection,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error(transparent)]
    Db(#[from] DbErr),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Model(#[from] crate::model::ModelError),
    #[error("invalid {field} value in database: {value}")]
    InvalidPort { field: &'static str, value: i32 },
    #[error(transparent)]
    PortConversion(#[from] TryFromIntError),
}

pub type Result<T> = std::result::Result<T, RepositoryError>;

impl Repository {
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(5000));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let db = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool.clone());
        let repo = Self { pool, db };
        repo.setup_schema().await?;
        repo.migrate_legacy_bindings().await?;
        Ok(repo)
    }

    pub async fn self_check(&self) -> Result<()> {
        let version: String = sqlx::query_scalar("SELECT sqlite_version();")
            .fetch_one(&self.pool)
            .await?;
        if version.is_empty() {
            return Err(sqlx::Error::RowNotFound.into());
        }
        Ok(())
    }

    pub async fn insert_allocation(&self, allocation: &Allocation) -> Result<()> {
        allocation_entity::ActiveModel {
            id: Set(allocation.id.clone()),
            protocol: Set(allocation.protocol.as_str().to_owned()),
            port: Set(i32::from(allocation.port)),
            target_port: Set(allocation.target_port.map(i32::from).unwrap_or(0)),
            host: Set(allocation.host.clone()),
            created_at_ms: Set(allocation.created_at_ms),
            updated_at_ms: Set(allocation.updated_at_ms),
        }
        .insert(&self.db)
        .await?;
        Ok(())
    }

    pub async fn put_binding(&self, binding: &Binding) -> Result<()> {
        match binding_entity::Entity::find_by_id(binding.allocation_id.clone())
            .one(&self.db)
            .await?
        {
            Some(model) => {
                let mut active = model.into_active_model();
                active.target_port = Set(i32::from(binding.target_port));
                active.host = Set(binding.host.clone());
                active.updated_at_ms = Set(binding.updated_at_ms);
                active.update(&self.db).await?;
            }
            None => {
                binding_entity::ActiveModel {
                    allocation_id: Set(binding.allocation_id.clone()),
                    target_port: Set(i32::from(binding.target_port)),
                    host: Set(binding.host.clone()),
                    created_at_ms: Set(binding.created_at_ms),
                    updated_at_ms: Set(binding.updated_at_ms),
                }
                .insert(&self.db)
                .await?;
            }
        }
        self.update_legacy_binding_columns(
            &binding.allocation_id,
            binding.target_port,
            binding.host.clone(),
            binding.updated_at_ms,
        )
        .await?;
        Ok(())
    }

    pub async fn delete_binding(&self, allocation_id: &str, updated_at_ms: i64) -> Result<bool> {
        let result = binding_entity::Entity::delete_by_id(allocation_id.to_owned())
            .exec(&self.db)
            .await?;
        let changed = result.rows_affected > 0;
        if changed {
            self.clear_legacy_binding_columns(allocation_id, updated_at_ms)
                .await?;
        }
        Ok(changed)
    }

    pub async fn delete_allocation(&self, id: &str) -> Result<bool> {
        binding_entity::Entity::delete_by_id(id.to_owned())
            .exec(&self.db)
            .await?;
        let result = allocation_entity::Entity::delete_by_id(id.to_owned())
            .exec(&self.db)
            .await?;
        Ok(result.rows_affected > 0)
    }

    pub async fn get_binding(&self, allocation_id: &str) -> Result<Option<Binding>> {
        binding_entity::Entity::find_by_id(allocation_id.to_owned())
            .one(&self.db)
            .await?
            .map(model_to_binding)
            .transpose()
    }

    pub async fn get_allocation(&self, id: &str) -> Result<Option<Allocation>> {
        let allocation = allocation_entity::Entity::find_by_id(id.to_owned())
            .one(&self.db)
            .await?;
        let Some(allocation) = allocation else {
            return Ok(None);
        };
        let binding = binding_entity::Entity::find_by_id(id.to_owned())
            .one(&self.db)
            .await?;
        Ok(Some(model_to_allocation(allocation, binding.as_ref())?))
    }

    pub async fn list_allocations(&self) -> Result<Vec<Allocation>> {
        let allocations = allocation_entity::Entity::find()
            .order_by_asc(allocation_entity::Column::Protocol)
            .order_by_asc(allocation_entity::Column::Port)
            .all(&self.db)
            .await?;
        let bindings = binding_entity::Entity::find().all(&self.db).await?;
        let bindings_by_id: HashMap<_, _> = bindings
            .iter()
            .map(|binding| (binding.allocation_id.as_str(), binding))
            .collect();

        allocations
            .into_iter()
            .map(|allocation| {
                let binding = bindings_by_id.get(allocation.id.as_str()).copied();
                model_to_allocation(allocation, binding)
            })
            .collect()
    }

    async fn setup_schema(&self) -> Result<()> {
        let builder = self.db.get_database_backend();
        let schema = Schema::new(builder);

        let mut allocations = schema.create_table_from_entity(allocation_entity::Entity);
        allocations.if_not_exists();
        self.db.execute(builder.build(&allocations)).await?;

        let mut bindings = schema.create_table_from_entity(binding_entity::Entity);
        bindings.if_not_exists();
        self.db.execute(builder.build(&bindings)).await?;

        let index = sea_orm::sea_query::Index::create()
            .if_not_exists()
            .name("idx_allocations_protocol_port")
            .table(allocation_entity::Entity)
            .col(allocation_entity::Column::Protocol)
            .col(allocation_entity::Column::Port)
            .unique()
            .to_owned();
        self.db.execute(builder.build(&index)).await?;
        Ok(())
    }

    async fn migrate_legacy_bindings(&self) -> Result<()> {
        self.db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO bindings(allocation_id, target_port, host, created_at_ms, updated_at_ms)
                 SELECT id, target_port, host, created_at_ms, updated_at_ms FROM allocations
                 WHERE target_port > 0
                   AND NOT EXISTS (SELECT 1 FROM bindings WHERE bindings.allocation_id = allocations.id);"
                    .to_owned(),
            ))
            .await?;
        Ok(())
    }

    async fn update_legacy_binding_columns(
        &self,
        allocation_id: &str,
        target_port: u16,
        host: Option<String>,
        updated_at_ms: i64,
    ) -> Result<()> {
        if let Some(model) = allocation_entity::Entity::find_by_id(allocation_id.to_owned())
            .one(&self.db)
            .await?
        {
            let mut active = model.into_active_model();
            active.target_port = Set(i32::from(target_port));
            active.host = Set(host);
            active.updated_at_ms = Set(updated_at_ms);
            active.update(&self.db).await?;
        }
        Ok(())
    }

    async fn clear_legacy_binding_columns(
        &self,
        allocation_id: &str,
        updated_at_ms: i64,
    ) -> Result<()> {
        if let Some(model) = allocation_entity::Entity::find_by_id(allocation_id.to_owned())
            .one(&self.db)
            .await?
        {
            let mut active = model.into_active_model();
            active.target_port = Set(0);
            active.host = Set(None);
            active.updated_at_ms = Set(updated_at_ms);
            active.update(&self.db).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn test_i64(&self, sql: &str, id: &str) -> i64 {
        sqlx::query_scalar(sql)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    #[cfg(test)]
    async fn test_optional_string(&self, sql: &str, id: &str) -> Option<String> {
        sqlx::query_scalar(sql)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    #[cfg(test)]
    async fn journal_mode(&self) -> String {
        sqlx::query_scalar("PRAGMA journal_mode;")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    #[cfg(test)]
    async fn busy_timeout_ms(&self) -> i64 {
        sqlx::query_scalar("PRAGMA busy_timeout;")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
}

fn model_to_allocation(
    allocation: allocation_entity::Model,
    binding: Option<&binding_entity::Model>,
) -> Result<Allocation> {
    let protocol = Protocol::from_str(&allocation.protocol)?;
    let port = port_from_i32("port", allocation.port)?;
    let target_port = match binding {
        Some(binding) => Some(port_from_i32("binding.target_port", binding.target_port)?),
        None if allocation.target_port > 0 => {
            Some(port_from_i32("target_port", allocation.target_port)?)
        }
        None => None,
    };
    let host = binding
        .and_then(|binding| binding.host.clone())
        .or(allocation.host);
    Ok(Allocation {
        id: allocation.id,
        protocol,
        port,
        target_port,
        host,
        created_at_ms: allocation.created_at_ms,
        updated_at_ms: allocation.updated_at_ms,
    })
}

fn model_to_binding(binding: binding_entity::Model) -> Result<Binding> {
    Ok(Binding {
        allocation_id: binding.allocation_id,
        target_port: port_from_i32("binding.target_port", binding.target_port)?,
        host: binding.host,
        created_at_ms: binding.created_at_ms,
        updated_at_ms: binding.updated_at_ms,
    })
}

fn port_from_i32(field: &'static str, value: i32) -> Result<u16> {
    if value < 0 {
        return Err(RepositoryError::InvalidPort { field, value });
    }
    u16::try_from(value).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Allocation, Binding, Protocol};
    use tempfile::NamedTempFile;

    async fn temp_repo() -> Repository {
        let file = NamedTempFile::new().unwrap();
        Repository::open(file.path()).await.unwrap()
    }

    fn allocation(
        id: &str,
        protocol: Protocol,
        port: u16,
        target_port: Option<u16>,
        host: Option<&str>,
    ) -> Allocation {
        Allocation {
            id: id.to_owned(),
            protocol,
            port,
            target_port,
            host: host.map(str::to_owned),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        }
    }

    #[tokio::test]
    async fn sqlite_self_check_wal_and_busy_timeout_succeed() {
        let repo = temp_repo().await;
        repo.self_check().await.unwrap();
        assert_eq!(repo.journal_mode().await.to_ascii_lowercase(), "wal");
        assert_eq!(repo.busy_timeout_ms().await, 5000);
    }

    #[tokio::test]
    async fn sqlite_persists_and_reloads_allocations_and_bindings() {
        let repo = temp_repo().await;
        let alloc = allocation("a1", Protocol::Tcp, 10000, None, None);
        repo.insert_allocation(&alloc).await.unwrap();
        assert_eq!(repo.get_allocation("a1").await.unwrap().unwrap(), alloc);

        let binding = Binding {
            allocation_id: "a1".to_owned(),
            target_port: 8080,
            host: Some("127.0.0.1".to_owned()),
            created_at_ms: 1100,
            updated_at_ms: 1100,
        };
        repo.put_binding(&binding).await.unwrap();
        assert_eq!(repo.get_binding("a1").await.unwrap().unwrap(), binding);
        assert_eq!(
            repo.test_i64("SELECT target_port FROM allocations WHERE id = ?", "a1")
                .await,
            8080
        );
        assert_eq!(
            repo.test_i64("SELECT updated_at_ms FROM allocations WHERE id = ?", "a1")
                .await,
            1100
        );
        assert_eq!(
            repo.test_optional_string("SELECT host FROM allocations WHERE id = ?", "a1")
                .await
                .as_deref(),
            Some("127.0.0.1")
        );
        let hydrated = repo.get_allocation("a1").await.unwrap().unwrap();
        assert_eq!(hydrated.target_port, Some(8080));
        assert_eq!(hydrated.host.as_deref(), Some("127.0.0.1"));

        let updated_binding = Binding {
            allocation_id: "a1".to_owned(),
            target_port: 9090,
            host: None,
            created_at_ms: 1100,
            updated_at_ms: 1150,
        };
        repo.put_binding(&updated_binding).await.unwrap();
        assert_eq!(
            repo.get_binding("a1").await.unwrap().unwrap(),
            updated_binding
        );
        assert_eq!(
            repo.test_i64("SELECT target_port FROM allocations WHERE id = ?", "a1")
                .await,
            9090
        );
        assert_eq!(
            repo.test_optional_string("SELECT host FROM allocations WHERE id = ?", "a1")
                .await,
            None
        );
        assert_eq!(
            repo.test_i64("SELECT updated_at_ms FROM allocations WHERE id = ?", "a1")
                .await,
            1150
        );

        assert!(repo.delete_binding("a1", 1200).await.unwrap());
        assert_eq!(
            repo.test_i64("SELECT target_port FROM allocations WHERE id = ?", "a1")
                .await,
            0
        );
        assert_eq!(
            repo.test_i64("SELECT updated_at_ms FROM allocations WHERE id = ?", "a1")
                .await,
            1200
        );
        assert_eq!(
            repo.test_optional_string("SELECT host FROM allocations WHERE id = ?", "a1")
                .await,
            None
        );
        let cleared = repo.get_allocation("a1").await.unwrap().unwrap();
        assert_eq!(cleared.target_port, None);
        assert_eq!(cleared.host, None);
        assert!(!repo.delete_binding("a1", 1300).await.unwrap());

        repo.put_binding(&binding).await.unwrap();
        assert!(repo.delete_allocation("a1").await.unwrap());
        assert!(repo.get_allocation("a1").await.unwrap().is_none());
        assert!(repo.get_binding("a1").await.unwrap().is_none());
        assert!(!repo.delete_allocation("a1").await.unwrap());
    }

    #[tokio::test]
    async fn sqlite_lists_allocations_ordered_by_protocol_then_port() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("udp", Protocol::Udp, 10002, None, None))
            .await
            .unwrap();
        repo.insert_allocation(&allocation("tcp", Protocol::Tcp, 10001, None, None))
            .await
            .unwrap();
        repo.insert_allocation(&allocation("both", Protocol::Both, 10000, None, None))
            .await
            .unwrap();
        let ids: Vec<String> = repo
            .list_allocations()
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(ids, vec!["both", "tcp", "udp"]);
    }

    #[tokio::test]
    async fn sqlite_migrates_legacy_allocation_binding_into_bindings_table() {
        let file = NamedTempFile::new().unwrap();
        {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(file.path())
                        .create_if_missing(true),
                )
                .await
                .unwrap();
            sqlx::query(
                "CREATE TABLE allocations (
                    id TEXT PRIMARY KEY,
                    protocol TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    target_port INTEGER NOT NULL,
                    host TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    UNIQUE(protocol, port)
                );",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO allocations(id, protocol, port, target_port, host, created_at_ms, updated_at_ms)
                 VALUES('legacy', 'tcp', 10000, 8080, '127.0.0.1', 10, 20);",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }
        let repo = Repository::open(file.path()).await.unwrap();
        let binding = repo.get_binding("legacy").await.unwrap().unwrap();
        assert_eq!(binding.target_port, 8080);
        assert_eq!(binding.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(binding.created_at_ms, 10);
        assert_eq!(binding.updated_at_ms, 20);
    }

    #[tokio::test]
    async fn sqlite_delete_allocation_removes_existing_binding_row() {
        let repo = temp_repo().await;
        repo.insert_allocation(&allocation("a1", Protocol::Tcp, 10000, None, None))
            .await
            .unwrap();
        repo.put_binding(&Binding {
            allocation_id: "a1".to_owned(),
            target_port: 8080,
            host: Some("127.0.0.1".to_owned()),
            created_at_ms: 1100,
            updated_at_ms: 1100,
        })
        .await
        .unwrap();

        assert!(repo.delete_allocation("a1").await.unwrap());

        assert_eq!(
            repo.test_i64(
                "SELECT COUNT(*) FROM bindings WHERE allocation_id = ?",
                "a1"
            )
            .await,
            0
        );
        assert!(repo.get_binding("a1").await.unwrap().is_none());
    }
}
