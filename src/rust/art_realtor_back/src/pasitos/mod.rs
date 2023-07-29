use super::*;

use futures::StreamExt;
pub mod elastic;
pub mod store;

pasitos!(fut_queue, run_for;
    init {
        let start = std::time::Instant::now();
        let opt = (*OPT.read().unwrap()).clone();
        let opt = opt.unwrap();
        match &opt.cmd {
            Some(Command::ForSite {
                facet
            }) => {
                if let Some(facet) = facet {
                    start_export(ExportFor::Site(*facet))?;
                } else {
                    start_export(ExportFor::Site(ForSiteFacet::Habit))?;
                    start_export(ExportFor::Site(ForSiteFacet::Cottage))?;
                }
            }
            Some(Command::ForAnalytics { }) => {
                start_export(ExportFor::Analytics)?;
            }
            None => {
                bail!("No command specified. Check with --help");
            }
        }
    }
    on_complete {
        info!(
            "{}, complete",
            arrange_millis::get(std::time::Instant::now().duration_since(start).as_millis()),
        );
        std::process::exit(0);
    }
    on_next_end {
    }
    demoras {
        demora ExportRetryFetch ({
            request: ElasticRequest,
            content: elastic_scan_specific::ElasticContent,
            mode: ExportFor,
        }) {
            pasitos!(elastic push_front Fetch {
                request,
                content,
                mode,
            });
        }
    }
    pasos elastic {
        max_at_once: settings!(elastic.max_at_once);
        paso Fetch ({
            request: ElasticRequest,
            content: elastic_scan_specific::ElasticContent,
            mode: ExportFor,
        }) -> ({
            res: pasitos::elastic::FetchResult,
            mode: ExportFor,
        }) {
            let res = pasitos::elastic::fetch(content, request).await;
        } => sync {
            pasitos::elastic::fetch_sync(res, mode)?;
        }
    }
    pasos store {
        max_at_once: 1;
        paso Save ({
            content_shared: elastic_scan_specific::ElasticContentShared,
            next_fetch: Option<crate::pasitos::pasos::elastic::Arg>,
        }) -> ({
            res: pasitos::store::SaveResult,
            content_shared: elastic_scan_specific::ElasticContentShared,
            next_fetch: Option<crate::pasitos::pasos::elastic::Arg>,
        }) {
            let res = pasitos::store::save(&content_shared).await;
        } => sync {
            pasitos::store::save_sync(res, content_shared, next_fetch)?;
        }
    }
);

fn start_export(mode: ExportFor) -> Result<()> {
    match mode {
        ExportFor::Analytics => {
            let facet = MlsFacet::MskHabitSale;
            let shared = std::sync::Arc::new(std::sync::RwLock::new(
                elastic_scan_specific::ElasticContentSharedInner::new(mode)?,
            ));
            let content = elastic_scan_specific::ElasticContent::new(facet, shared);
            let request = content.new_request(mode);
            pasitos!(elastic push_back Fetch { request, content, mode });
        }
        ExportFor::Site(facet) => {
            let facet = MlsFacet::from(facet);
            let shared = std::sync::Arc::new(std::sync::RwLock::new(
                elastic_scan_specific::ElasticContentSharedInner::new(mode)?,
            ));
            let content = elastic_scan_specific::ElasticContent::new(facet, shared);
            let request = content.new_request(mode);
            pasitos!(elastic push_back Fetch { request, content, mode });
        }
    }
    Ok(())
}
