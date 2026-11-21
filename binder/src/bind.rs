use std::collections::{HashMap, HashSet};
use std::time::Duration;

use lapin::options::BasicAckOptions;
use lapin::types::DeliveryTag;
use sqlx::{Pool, Postgres};
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::models::company::NewCompanySnapshot;
use crate::models::job_ad::NewJobAd;
use crate::models::messages::CommonJobAdDto;
use crate::models::skill::NewSkillSnapshot;
use crate::repositories::{
    company::CompanyRepository, company_snapshot::CompanySnapshotRepository,
    company_variant::CompanyVariantRepository, job_ad::JobAdRepository,
    job_ad_skill::JobAdSkillRepository, skill::SkillRepository,
    skill_snapshot::SkillSnapshotRepository, skill_variant::SkillVariantRepository,
};

const IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const FAILED_PAYLOAD_DIR: &str = "failed_payloads";
const MAX_BUFFER_SIZE: usize = 3000;
// max unacked deliveries RabbitMQ hands us, bounds the buffer while the database is down
pub const PREFETCH_COUNT: u16 = 16;
const RETRY_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_MAX: Duration = Duration::from_mins(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FAILURES_WITH_DB_UP: u32 = 3;

// job ads that are received but not yet committed, their deliveries stay unacked
#[derive(Default)]
struct Buffer {
    job_ads: Vec<CommonJobAdDto>,
    deliveries: u16,
    last_delivery_tag: Option<DeliveryTag>,
}

pub async fn bind(
    pool: &Pool<Postgres>,
    channel: &lapin::Channel,
    mut consumer: lapin::Consumer,
) -> color_eyre::Result<()> {
    std::fs::create_dir(FAILED_PAYLOAD_DIR)
        .unwrap_or_else(|e| warn!("Failed to create failed payloads directory: {e}"));

    let mut buffer = Buffer::default();

    loop {
        tokio::select! {
            delivery = consumer.next() => {
                let Some(delivery) = delivery else {
                    info!(
                        "Consumer stream ended, {} buffered job ads stay unacked and will be redelivered",
                        buffer.job_ads.len()
                    );
                    break;
                };
                let delivery = delivery?;
                let payload = std::str::from_utf8(&delivery.data)?;

                let job_ads: Vec<CommonJobAdDto> = match serde_json::from_str(payload) {
                    Ok(ads) => ads,
                    Err(e) => {
                        std::fs::write(format!("{}/{}.txt", FAILED_PAYLOAD_DIR, chrono::Utc::now().timestamp()), payload)?;
                        warn!("Failed to deserialize payload: {e}");
                        delivery
                            .ack(BasicAckOptions::default())
                            .await?;
                        continue;
                    }
                };

                info!("Received {} job ads", job_ads.len());

                buffer.job_ads.extend(job_ads);
                buffer.deliveries += 1;
                buffer.last_delivery_tag = Some(delivery.delivery_tag);

                if buffer.job_ads.len() >= MAX_BUFFER_SIZE || buffer.deliveries >= PREFETCH_COUNT {
                    info!("Buffer full, flushing {} job ads to database", buffer.job_ads.len());
                    flush(pool, channel, &mut buffer).await?;
                }
            }
            () = tokio::time::sleep(IDLE_TIMEOUT) => {
                if buffer.last_delivery_tag.is_none() {
                    continue;
                }

                info!("Idle timeout reached, flushing {} job ads to database", buffer.job_ads.len());
                flush(pool, channel, &mut buffer).await?;
            }
        }
    }

    Ok(())
}

async fn flush(
    pool: &Pool<Postgres>,
    channel: &lapin::Channel,
    buffer: &mut Buffer,
) -> color_eyre::Result<()> {
    let Some(delivery_tag) = buffer.last_delivery_tag else {
        return Ok(());
    };

    if !buffer.job_ads.is_empty() {
        let mut backoff = RETRY_BACKOFF_MIN;
        let mut failures_with_db_up = 0;
        while let Err(e) = upsert_data(&buffer.job_ads, pool).await {
            // a failure while the database answers is caused by the batch, not by an outage
            if database_is_up(pool).await {
                failures_with_db_up += 1;
                if failures_with_db_up >= MAX_FAILURES_WITH_DB_UP {
                    return Err(e.into());
                }
            }

            warn!(
                "Flush of {} job ads failed, retrying in {}s: {e}",
                buffer.job_ads.len(),
                backoff.as_secs()
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
        }
    }

    channel
        .basic_ack(delivery_tag, BasicAckOptions { multiple: true })
        .await?;
    *buffer = Buffer::default();

    Ok(())
}

async fn database_is_up(pool: &Pool<Postgres>) -> bool {
    let probe = sqlx::query("SELECT 1").execute(pool);

    matches!(tokio::time::timeout(PROBE_TIMEOUT, probe).await, Ok(Ok(_)))
}

#[allow(clippy::too_many_lines)]
async fn upsert_data(data: &[CommonJobAdDto], pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    let start = std::time::Instant::now();
    let mut tx = pool.begin().await?;
    let external_ids: Vec<String> = data.iter().map(|d| d.id.clone()).collect();

    let (
        existing_job_ads,
        all_skills,
        all_skill_variants,
        all_skill_snapshots,
        all_companies,
        all_company_variants,
        all_company_snapshots,
    ) = tokio::try_join!(
        JobAdRepository::get_by_external_ids(pool, &external_ids),
        SkillRepository::get_all(pool),
        SkillVariantRepository::get_all(pool),
        SkillSnapshotRepository::get_all(pool),
        CompanyRepository::get_all(pool),
        CompanyVariantRepository::get_all(pool),
        CompanySnapshotRepository::get_all(pool),
    )?;

    let existing_job_ads: HashSet<(_, &str)> = existing_job_ads
        .iter()
        .map(|ja| (ja.job_site, ja.external_id.as_str()))
        .collect();

    // prepare lookups for companies
    let mut company_by_variant = HashMap::new();

    for company in &all_companies {
        company_by_variant.insert(&company.name, company.company_id);
    }
    for cv in &all_company_variants {
        company_by_variant.insert(&cv.name, cv.company_id);
    }

    // prepare lookups for company snapshots
    let mut company_snapshot_by_name = all_company_snapshots
        .iter()
        .map(|cs| (&cs.name, cs.company_snapshot_id))
        .collect::<HashMap<&String, i32>>();

    // prepare lookups for skill
    let mut skill_by_variant: HashMap<&String, i32> = HashMap::new();
    for skill in &all_skills {
        skill_by_variant.insert(&skill.name, skill.skill_id);
    }
    for sv in &all_skill_variants {
        skill_by_variant.insert(&sv.name, sv.skill_id);
    }

    // prepare lookups for skill snapshots
    let mut skill_snapshot_by_name: HashMap<&String, i32> = all_skill_snapshots
        .iter()
        .map(|ss| (&ss.name, ss.skill_snapshot_id))
        .collect();

    // skip job ads already in the database and duplicates within the batch
    let mut seen = HashSet::new();
    let new_data: Vec<&CommonJobAdDto> = data
        .iter()
        .filter(|ja| !existing_job_ads.contains(&(ja.source_site, ja.id.as_str())))
        .filter(|ja| seen.insert((ja.source_site, ja.id.as_str())))
        .collect();

    // main uspert loop
    for job_ad_to_insert in new_data.iter().copied() {
        let mut new_job_ad = NewJobAd::from_message(job_ad_to_insert);
        // handle company if exists
        new_job_ad.company_id = company_by_variant
            .get(&job_ad_to_insert.company_name)
            .copied();

        // None means another writer inserted this job ad in the meantime
        let Some(new_job_ad_id) = JobAdRepository::insert(&mut *tx, &new_job_ad).await? else {
            continue;
        };

        // If no company exists, check if a snapshot exists
        if new_job_ad.company_id.is_none() {
            if let Some(company_snapshot_id) =
                company_snapshot_by_name.get(&job_ad_to_insert.company_name)
            {
                CompanySnapshotRepository::append_job_ad_id(
                    &mut *tx,
                    *company_snapshot_id,
                    new_job_ad_id,
                )
                .await?;
            } else {
                // If no snapshot exists, insert a new company snapshot
                let new_company_snapshot = NewCompanySnapshot {
                    name: job_ad_to_insert.company_name.clone(),
                    job_ad_ids: vec![new_job_ad_id],
                };
                let new_company_snapshot_id =
                    CompanySnapshotRepository::insert(&mut *tx, &new_company_snapshot).await?;
                company_snapshot_by_name
                    .insert(&job_ad_to_insert.company_name, new_company_snapshot_id);
            }
        }

        // handle skills if exists
        if let Some(ja_skills) = &job_ad_to_insert.skills {
            for ja_skill in ja_skills {
                // if the skills exists append new pair (job_ad, skill_id)
                if let Some(&skill_id) = skill_by_variant.get(ja_skill) {
                    JobAdSkillRepository::insert(&mut *tx, new_job_ad_id, skill_id).await?;
                }
                // if the skill snapshot exists, append new job_ad_id to the snapshot
                else if let Some(skill_snapshot_id) = skill_snapshot_by_name.get(&ja_skill) {
                    SkillSnapshotRepository::append_skill_snapshot_job_ad_id(
                        &mut *tx,
                        *skill_snapshot_id,
                        new_job_ad_id,
                    )
                    .await?;
                }
                // if no skill snapshot exists, appen a new skill snapshot
                else {
                    let new_skill_snapshot = NewSkillSnapshot {
                        name: ja_skill.clone(),
                        job_ad_ids: vec![new_job_ad_id],
                    };
                    let new_skill_snapshot_id =
                        SkillSnapshotRepository::insert(&mut *tx, &new_skill_snapshot).await?;
                    skill_snapshot_by_name.insert(ja_skill, new_skill_snapshot_id);
                }
            }
        }
    }

    tx.commit().await?;

    info!(
        "Upsert complete: {} total, {} new in {}ms.",
        data.len(),
        new_data.len(),
        start.elapsed().as_millis()
    );
    Ok(())
}
